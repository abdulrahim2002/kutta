package main

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"math"
	"net/http"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"time"

	"github.com/gorilla/websocket"
)

const (
	defaultRobotIP      = "10.174.16.11"
	defaultPort         = "8080"
	defaultAgentModel   = "gpt-realtime-2"
	realtimeWSBase      = "wss://api.openai.com/v1/realtime?model=%s"
	agentAskTimeout     = 22 * time.Second
	agentSetupTimeout   = 16 * time.Second
	agentWriteTimeout   = 12 * time.Second
	agentImageMinGap    = 1 * time.Second
	goalTickInterval    = 1 * time.Second
	minCommandDuration  = 150 * time.Millisecond * 5
	maxCommandDuration  = 2500 * time.Millisecond * 5
	defaultCommandDrive = 650 * time.Millisecond * 5
	rotatePollInterval  = 90 * time.Millisecond
	rotateSettleDelay   = 80 * time.Millisecond
	rotateMinStep       = 120 * time.Millisecond
	rotateMaxStep       = 560 * time.Millisecond
	rotateMaxDuration   = 14 * time.Second
)

var allowedCommands = map[string]struct{}{
	"forward":  {},
	"backward": {},
	"left":     {},
	"right":    {},
	"stop":     {},
	"laydown":  {},
}

type workerRequest struct {
	ID     int64  `json:"id"`
	Action string `json:"action"`
	Params any    `json:"params,omitempty"`
}

type workerResponse struct {
	ID    int64           `json:"id"`
	OK    bool            `json:"ok"`
	Data  json.RawMessage `json:"data,omitempty"`
	Error string          `json:"error,omitempty"`
}

type workerStatus struct {
	Connected bool   `json:"connected"`
	IP        string `json:"ip,omitempty"`
}

type workerOrientation struct {
	Connected   bool    `json:"connected"`
	IP          string  `json:"ip,omitempty"`
	Valid       bool    `json:"valid"`
	YawRad      float64 `json:"yaw_rad"`
	RollRad     float64 `json:"roll_rad"`
	PitchRad    float64 `json:"pitch_rad"`
	YawSpeed    float64 `json:"yaw_speed"`
	Source      string  `json:"source,omitempty"`
	TimestampMS int64   `json:"timestamp_ms,omitempty"`
}

type videoFrame struct {
	Connected    bool   `json:"connected"`
	VideoEnabled bool   `json:"video_enabled"`
	ImageB64     string `json:"image_b64,omitempty"`
	TimestampMS  int64  `json:"timestamp_ms,omitempty"`
	Error        string `json:"error,omitempty"`
}

type workerClient struct {
	mu         sync.Mutex
	scriptPath string
	pythonBin  string

	cmd    *exec.Cmd
	stdin  io.WriteCloser
	stdout *bufio.Scanner
	nextID int64
}

func newWorkerClient(scriptPath string) *workerClient {
	pythonBin := strings.TrimSpace(os.Getenv("UNITREE_PYTHON_BIN"))
	if pythonBin == "" {
		pythonBin = "python"
	}
	return &workerClient{scriptPath: scriptPath, pythonBin: pythonBin}
}

func (c *workerClient) startLocked() error {
	if c.cmd != nil {
		return nil
	}

	cmd := exec.Command(c.pythonBin, c.scriptPath)
	stdin, err := cmd.StdinPipe()
	if err != nil {
		return fmt.Errorf("failed to open worker stdin: %w", err)
	}

	stdout, err := cmd.StdoutPipe()
	if err != nil {
		return fmt.Errorf("failed to open worker stdout: %w", err)
	}

	cmd.Stderr = os.Stderr
	if err := cmd.Start(); err != nil {
		return fmt.Errorf("failed to start worker with %q: %w", c.pythonBin, err)
	}

	scanner := bufio.NewScanner(stdout)
	scanner.Buffer(make([]byte, 1024), 6*1024*1024)

	c.cmd = cmd
	c.stdin = stdin
	c.stdout = scanner
	return nil
}

func (c *workerClient) cleanupLocked() {
	if c.stdin != nil {
		_ = c.stdin.Close()
	}
	c.cmd = nil
	c.stdin = nil
	c.stdout = nil
}

func (c *workerClient) call(action string, params any, out any) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if err := c.startLocked(); err != nil {
		return err
	}

	c.nextID++
	req := workerRequest{ID: c.nextID, Action: action, Params: params}
	payload, err := json.Marshal(req)
	if err != nil {
		return fmt.Errorf("failed to encode worker request: %w", err)
	}

	if _, err := c.stdin.Write(append(payload, '\n')); err != nil {
		c.cleanupLocked()
		return fmt.Errorf("failed to write request to worker: %w", err)
	}

	if !c.stdout.Scan() {
		scanErr := c.stdout.Err()
		c.cleanupLocked()
		if scanErr != nil {
			return fmt.Errorf("failed to read worker response: %w", scanErr)
		}
		return errors.New("worker stopped unexpectedly")
	}

	line := c.stdout.Bytes()
	var response workerResponse
	if err := json.Unmarshal(line, &response); err != nil {
		return fmt.Errorf("worker returned invalid response: %s", strings.TrimSpace(string(line)))
	}

	if !response.OK {
		if response.Error == "" {
			response.Error = "unknown worker error"
		}
		return errors.New(response.Error)
	}

	if out != nil && len(response.Data) > 0 {
		if err := json.Unmarshal(response.Data, out); err != nil {
			return fmt.Errorf("failed to decode worker payload: %w", err)
		}
	}

	return nil
}

func (c *workerClient) close() {
	_ = c.call("shutdown", nil, nil)

	c.mu.Lock()
	defer c.mu.Unlock()
	if c.cmd != nil && c.cmd.Process != nil {
		_ = c.cmd.Process.Kill()
	}
	c.cleanupLocked()
}

type agentStatus struct {
	Running       bool   `json:"running"`
	Model         string `json:"model,omitempty"`
	GoalActive    bool   `json:"goal_active"`
	GoalTarget    string `json:"goal_target,omitempty"`
	LastAction    string `json:"last_action,omitempty"`
	LastReply     string `json:"last_reply,omitempty"`
	LastError     string `json:"last_error,omitempty"`
	LastInputAt   string `json:"last_input_at,omitempty"`
	LastUpdatedAt string `json:"last_updated_at,omitempty"`
}

type toolCall struct {
	Name      string
	CallID    string
	Arguments string
}

type realtimeAgent struct {
	worker *workerClient

	mu sync.Mutex

	conn            *websocket.Conn
	eventCh         chan map[string]any
	readErrCh       chan error
	apiKey          string
	model           string
	lastFrameSentAt time.Time
	goalCancel      context.CancelFunc
	status          agentStatus
}

func newRealtimeAgent(worker *workerClient) *realtimeAgent {
	return &realtimeAgent{
		worker: worker,
		status: agentStatus{
			Running:       false,
			Model:         defaultAgentModel,
			GoalActive:    false,
			LastAction:    "idle",
			LastReply:     "",
			LastError:     "",
			LastInputAt:   "",
			LastUpdatedAt: time.Now().Format(time.RFC3339),
		},
	}
}

func (a *realtimeAgent) snapshot() agentStatus {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.status
}

func (a *realtimeAgent) markUpdatedLocked() {
	a.status.LastUpdatedAt = time.Now().Format(time.RFC3339)
}

func (a *realtimeAgent) writeJSONLocked(v any) error {
	if a.conn == nil {
		return errors.New("agent websocket is not connected")
	}
	_ = a.conn.SetWriteDeadline(time.Now().Add(agentWriteTimeout))
	err := a.conn.WriteJSON(v)
	_ = a.conn.SetWriteDeadline(time.Time{})
	return err
}

func (a *realtimeAgent) startReadLoopLocked() {
	if a.conn == nil {
		return
	}

	eventCh := make(chan map[string]any, 256)
	readErrCh := make(chan error, 1)
	conn := a.conn

	a.eventCh = eventCh
	a.readErrCh = readErrCh

	go func() {
		for {
			_, raw, err := conn.ReadMessage()
			if err != nil {
				select {
				case readErrCh <- err:
				default:
				}
				close(eventCh)
				return
			}

			var evt map[string]any
			if err := json.Unmarshal(raw, &evt); err != nil {
				continue
			}

			select {
			case eventCh <- evt:
			default:
				// Keep the stream live under bursty event traffic by dropping one old event.
				select {
				case <-eventCh:
				default:
				}
				select {
				case eventCh <- evt:
				default:
				}
			}
		}
	}()
}

func (a *realtimeAgent) drainEventsLocked() {
	if a.eventCh == nil {
		return
	}
	for {
		select {
		case _, ok := <-a.eventCh:
			if !ok {
				return
			}
		default:
			return
		}
	}
}

func normalizeCommand(raw string) string {
	cmd := strings.ToLower(strings.TrimSpace(raw))
	if _, ok := allowedCommands[cmd]; ok {
		return cmd
	}
	return ""
}

func chooseModel() string {
	model := strings.TrimSpace(os.Getenv("OPENAI_REALTIME_MODEL"))
	if model == "" {
		model = strings.TrimSpace(os.Getenv("OPENAI_MODEL"))
	}
	if model == "" {
		model = defaultAgentModel
	}
	return model
}

func (a *realtimeAgent) resetLocked() {
	if a.goalCancel != nil {
		a.goalCancel()
		a.goalCancel = nil
	}
	a.eventCh = nil
	a.readErrCh = nil
	if a.conn != nil {
		_ = a.conn.WriteControl(websocket.CloseMessage, websocket.FormatCloseMessage(websocket.CloseNormalClosure, ""), time.Now().Add(2*time.Second))
		_ = a.conn.Close()
		a.conn = nil
	}
	a.lastFrameSentAt = time.Time{}
	a.status.GoalActive = false
	a.status.GoalTarget = ""
	a.status.Running = false
	a.status.LastAction = "stopped"
	a.markUpdatedLocked()
}

func (a *realtimeAgent) Start(apiKey string) error {
	key := strings.TrimSpace(apiKey)
	if key == "" {
		key = strings.TrimSpace(os.Getenv("OPENAI_API_KEY"))
	}
	if key == "" {
		return errors.New("OPENAI_API_KEY is required")
	}

	model := chooseModel()
	wsURL := fmt.Sprintf(realtimeWSBase, url.QueryEscape(model))

	header := http.Header{}
	header.Set("Authorization", "Bearer "+key)
	if sid := strings.TrimSpace(os.Getenv("OPENAI_SAFETY_IDENTIFIER")); sid != "" {
		header.Set("OpenAI-Safety-Identifier", sid)
	}

	dialer := websocket.Dialer{HandshakeTimeout: agentSetupTimeout}
	conn, resp, err := dialer.Dial(wsURL, header)
	if err != nil {
		if resp != nil {
			return fmt.Errorf("realtime dial failed: %w (http %s)", err, resp.Status)
		}
		return fmt.Errorf("realtime dial failed: %w", err)
	}

	sessionUpdate := map[string]any{
		"type": "session.update",
		"session": map[string]any{
			"type":  "realtime",
			"model": model,
			"reasoning": map[string]any{
				"effort": "high",
			},
			"output_modalities": []string{"text"},
			"instructions": strings.TrimSpace(
				"You are the single control agent for a Unitree Go2 robot. " +
					"Use tools for commands and goal control. " +
					"For direct movement, call robot_command. " +
					"For angle-precise turns, call robot_rotate. " +
					"For long tasks like 'go to object', call start_object_goal. " +
					"For user questions about the current view, answer clearly and concisely from the provided image. " +
					"If uncertain about scene details, say so.",
			),
			"tool_choice": "auto",
			"tools": []map[string]any{
				{
					"type":        "function",
					"name":        "robot_command",
					"description": "Send one robot motion command. Use short durations for incremental motion.",
					"parameters": map[string]any{
						"type": "object",
						"properties": map[string]any{
							"command": map[string]any{
								"type": "string",
								"enum": []string{"forward", "backward", "left", "right", "stop", "laydown"},
							},
							"duration_ms": map[string]any{
								"type":        "integer",
								"description": "Optional duration for movement commands. Ignored for stop/laydown.",
							},
						},
						"required": []string{"command"},
					},
				},
				{
					"type":        "function",
					"name":        "robot_rotate",
					"description": "Rotate robot by a relative yaw angle using closed-loop orientation feedback from the robot state stream.",
					"parameters": map[string]any{
						"type": "object",
						"properties": map[string]any{
							"angle_deg": map[string]any{
								"type":        "number",
								"description": "Relative rotation angle in degrees. Positive turns left, negative turns right.",
							},
							"tolerance_deg": map[string]any{
								"type":        "number",
								"description": "Optional completion tolerance in degrees (default 7.5).",
							},
							"max_duration_ms": map[string]any{
								"type":        "integer",
								"description": "Optional timeout budget for this rotation.",
							},
						},
						"required": []string{"angle_deg"},
					},
				},
				{
					"type":        "function",
					"name":        "start_object_goal",
					"description": "Start autonomous object-goal navigation loop.",
					"parameters": map[string]any{
						"type": "object",
						"properties": map[string]any{
							"target": map[string]any{
								"type":        "string",
								"description": "Object name to reach, like bottle, chair, box.",
							},
						},
						"required": []string{"target"},
					},
				},
				{
					"type":        "function",
					"name":        "stop_object_goal",
					"description": "Stop autonomous object-goal navigation loop immediately.",
					"parameters": map[string]any{
						"type":       "object",
						"properties": map[string]any{},
					},
				},
				{
					"type":        "function",
					"name":        "get_robot_status",
					"description": "Get robot connection and goal status.",
					"parameters": map[string]any{
						"type":       "object",
						"properties": map[string]any{},
					},
				},
			},
		},
	}

	_ = conn.SetWriteDeadline(time.Now().Add(agentWriteTimeout))
	if err := conn.WriteJSON(sessionUpdate); err != nil {
		_ = conn.Close()
		return fmt.Errorf("realtime session.update failed: %w", err)
	}
	_ = conn.SetWriteDeadline(time.Time{})

	setupDeadline := time.Now().Add(agentSetupTimeout)
	for {
		_ = conn.SetReadDeadline(setupDeadline)
		_, raw, err := conn.ReadMessage()
		if err != nil {
			_ = conn.Close()
			return fmt.Errorf("realtime setup read failed: %w", err)
		}

		var evt map[string]any
		if err := json.Unmarshal(raw, &evt); err != nil {
			continue
		}

		t, _ := evt["type"].(string)
		if t == "session.updated" {
			break
		}
		if t == "error" {
			_ = conn.Close()
			return fmt.Errorf("realtime setup error: %s", parseErrorMessage(evt))
		}
	}
	// Clear temporary setup read deadline; the persistent read loop should not
	// inherit this timeout.
	_ = conn.SetReadDeadline(time.Time{})

	a.mu.Lock()
	defer a.mu.Unlock()
	a.resetLocked()
	a.conn = conn
	a.apiKey = key
	a.model = model
	a.status.Running = true
	a.status.Model = model
	a.status.LastAction = "agent_started"
	a.status.LastError = ""
	a.markUpdatedLocked()
	a.startReadLoopLocked()
	return nil
}

func (a *realtimeAgent) Stop() {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.resetLocked()
}

func (a *realtimeAgent) StopGoal() {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.stopGoalLocked(true)
}

func (a *realtimeAgent) stopGoalLocked(sendStop bool) {
	if a.goalCancel != nil {
		a.goalCancel()
		a.goalCancel = nil
	}
	a.status.GoalActive = false
	a.status.GoalTarget = ""
	if sendStop {
		_ = a.worker.call("command", map[string]string{"command": "stop"}, nil)
	}
	a.status.LastAction = "goal_stopped"
	a.markUpdatedLocked()
}

func (a *realtimeAgent) startGoalLocked(target string) {
	if a.goalCancel != nil {
		a.goalCancel()
		a.goalCancel = nil
	}

	ctx, cancel := context.WithCancel(context.Background())
	a.goalCancel = cancel
	a.status.GoalActive = true
	a.status.GoalTarget = target
	a.status.LastAction = "goal_started"
	a.markUpdatedLocked()

	go a.goalLoop(ctx, target)
}

func clampDuration(ms int) time.Duration {
	if ms <= 0 {
		return defaultCommandDrive
	}
	d := time.Duration(ms) * time.Millisecond
	if d < minCommandDuration {
		return minCommandDuration
	}
	if d > maxCommandDuration {
		return maxCommandDuration
	}
	return d
}

func normalizeAngleRad(angle float64) float64 {
	for angle > math.Pi {
		angle -= 2.0 * math.Pi
	}
	for angle < -math.Pi {
		angle += 2.0 * math.Pi
	}
	return angle
}

func radiansToDegrees(rad float64) float64 {
	return rad * 180.0 / math.Pi
}

func clampRotateToleranceDeg(value float64) float64 {
	if value == 0 {
		return 7.5
	}
	if value < 2.0 {
		return 2.0
	}
	if value > 25.0 {
		return 25.0
	}
	return value
}

func rotateStepDuration(remainingDeg float64) time.Duration {
	switch {
	case remainingDeg <= 10.0:
		return rotateMinStep
	case remainingDeg >= 75.0:
		return rotateMaxStep
	default:
		rangeDeg := 75.0 - 10.0
		f := (remainingDeg - 10.0) / rangeDeg
		return rotateMinStep + time.Duration(f*float64(rotateMaxStep-rotateMinStep))
	}
}

func rotateTimeoutBudget(angleDeg float64, maxDurationMS int) time.Duration {
	defaultBudget := time.Duration(2200+int(math.Abs(angleDeg)*38.0)) * time.Millisecond
	if defaultBudget < 2*time.Second {
		defaultBudget = 2 * time.Second
	}
	if defaultBudget > rotateMaxDuration {
		defaultBudget = rotateMaxDuration
	}

	if maxDurationMS <= 0 {
		return defaultBudget
	}

	override := time.Duration(maxDurationMS) * time.Millisecond
	if override < 1200*time.Millisecond {
		override = 1200 * time.Millisecond
	}
	if override > rotateMaxDuration {
		override = rotateMaxDuration
	}
	return override
}

func (a *realtimeAgent) readOrientationLocked() (workerOrientation, error) {
	var orientation workerOrientation
	if err := a.worker.call("orientation", nil, &orientation); err != nil {
		return orientation, err
	}
	if !orientation.Connected {
		return orientation, errors.New("robot is not connected")
	}
	if !orientation.Valid {
		return orientation, errors.New("orientation is not ready yet")
	}
	orientation.YawRad = normalizeAngleRad(orientation.YawRad)
	return orientation, nil
}

func (a *realtimeAgent) waitOrientationLocked(timeout time.Duration) (workerOrientation, error) {
	deadline := time.Now().Add(timeout)
	var lastErr error
	for {
		orientation, err := a.readOrientationLocked()
		if err == nil {
			return orientation, nil
		}
		lastErr = err
		if time.Now().After(deadline) {
			break
		}
		time.Sleep(rotatePollInterval)
	}
	return workerOrientation{}, lastErr
}

func (a *realtimeAgent) rotateByAngleLocked(angleDeg float64, toleranceDeg float64, maxDurationMS int) (map[string]any, error) {
	if math.IsNaN(angleDeg) || math.IsInf(angleDeg, 0) {
		return nil, errors.New("robot_rotate: angle_deg must be a finite number")
	}
	if angleDeg > 360.0 || angleDeg < -360.0 {
		return nil, errors.New("robot_rotate: angle_deg must be between -360 and 360")
	}

	tolerance := clampRotateToleranceDeg(toleranceDeg)
	if math.Abs(angleDeg) <= tolerance {
		_ = a.worker.call("command", map[string]string{"command": "stop"}, nil)
		return map[string]any{
			"ok":              true,
			"rotated_deg":     0.0,
			"requested_deg":   angleDeg,
			"final_error_deg": math.Abs(angleDeg),
			"target_reached":  true,
		}, nil
	}

	start, err := a.waitOrientationLocked(3 * time.Second)
	if err != nil {
		return nil, fmt.Errorf("robot_rotate: %w", err)
	}

	targetYaw := normalizeAngleRad(start.YawRad + (angleDeg * math.Pi / 180.0))
	deadline := time.Now().Add(rotateTimeoutBudget(angleDeg, maxDurationMS))
	steps := 0
	lastRemaining := 180.0

	for time.Now().Before(deadline) {
		current, err := a.readOrientationLocked()
		if err != nil {
			return nil, fmt.Errorf("robot_rotate orientation read failed: %w", err)
		}

		remainingRad := normalizeAngleRad(targetYaw - current.YawRad)
		remainingDeg := math.Abs(radiansToDegrees(remainingRad))
		lastRemaining = remainingDeg
		if remainingDeg <= tolerance {
			_ = a.worker.call("command", map[string]string{"command": "stop"}, nil)
			actualTurnDeg := radiansToDegrees(normalizeAngleRad(current.YawRad - start.YawRad))
			a.status.LastAction = fmt.Sprintf("tool_robot_rotate_done_%.1fdeg", actualTurnDeg)
			a.markUpdatedLocked()
			return map[string]any{
				"ok":              true,
				"requested_deg":   angleDeg,
				"rotated_deg":     actualTurnDeg,
				"final_error_deg": remainingDeg,
				"target_reached":  true,
				"steps":           steps,
			}, nil
		}

		command := "left"
		if remainingRad < 0.0 {
			command = "right"
		}
		if err := a.worker.call("command", map[string]string{"command": command}, nil); err != nil {
			return nil, fmt.Errorf("robot_rotate command failed: %w", err)
		}

		time.Sleep(rotateStepDuration(remainingDeg))
		_ = a.worker.call("command", map[string]string{"command": "stop"}, nil)
		time.Sleep(rotateSettleDelay)
		steps++
	}

	_ = a.worker.call("command", map[string]string{"command": "stop"}, nil)
	a.status.LastAction = "tool_robot_rotate_timeout"
	a.markUpdatedLocked()
	return nil, fmt.Errorf("robot_rotate timeout: remaining %.1f deg", lastRemaining)
}

func (a *realtimeAgent) executeToolLocked(call toolCall) (string, error) {
	switch call.Name {
	case "robot_command":
		var args struct {
			Command    string `json:"command"`
			DurationMS int    `json:"duration_ms,omitempty"`
		}
		if err := json.Unmarshal([]byte(call.Arguments), &args); err != nil {
			return "", fmt.Errorf("robot_command args invalid: %w", err)
		}

		cmd := normalizeCommand(args.Command)
		if cmd == "" {
			return "", errors.New("robot_command: unsupported command")
		}

		if err := a.worker.call("command", map[string]string{"command": cmd}, nil); err != nil {
			return "", err
		}

		if cmd != "stop" && cmd != "laydown" {
			time.Sleep(clampDuration(args.DurationMS))
			_ = a.worker.call("command", map[string]string{"command": "stop"}, nil)
		}

		a.status.LastAction = "tool_robot_command_" + cmd
		a.markUpdatedLocked()

		return mustJSON(map[string]any{
			"ok":      true,
			"command": cmd,
		}), nil

	case "robot_rotate":
		var args struct {
			AngleDeg      float64 `json:"angle_deg"`
			ToleranceDeg  float64 `json:"tolerance_deg,omitempty"`
			MaxDurationMS int     `json:"max_duration_ms,omitempty"`
		}
		if err := json.Unmarshal([]byte(call.Arguments), &args); err != nil {
			return "", fmt.Errorf("robot_rotate args invalid: %w", err)
		}

		result, err := a.rotateByAngleLocked(args.AngleDeg, args.ToleranceDeg, args.MaxDurationMS)
		if err != nil {
			return "", err
		}
		return mustJSON(result), nil

	case "start_object_goal":
		var args struct {
			Target string `json:"target"`
		}
		if err := json.Unmarshal([]byte(call.Arguments), &args); err != nil {
			return "", fmt.Errorf("start_object_goal args invalid: %w", err)
		}
		target := strings.TrimSpace(strings.ToLower(args.Target))
		if target == "" {
			return "", errors.New("start_object_goal: target is required")
		}

		a.startGoalLocked(target)
		return mustJSON(map[string]any{
			"ok":          true,
			"goal_active": true,
			"target":      target,
		}), nil

	case "stop_object_goal":
		a.stopGoalLocked(true)
		return mustJSON(map[string]any{
			"ok":          true,
			"goal_active": false,
		}), nil

	case "get_robot_status":
		var robot workerStatus
		if err := a.worker.call("status", nil, &robot); err != nil {
			return "", err
		}

		orientationData := map[string]any{
			"valid": false,
		}
		var orientation workerOrientation
		if err := a.worker.call("orientation", nil, &orientation); err == nil {
			orientationData = map[string]any{
				"valid":        orientation.Valid,
				"yaw_deg":      radiansToDegrees(normalizeAngleRad(orientation.YawRad)),
				"yaw_speed":    orientation.YawSpeed,
				"source":       orientation.Source,
				"timestamp_ms": orientation.TimestampMS,
			}
		}

		return mustJSON(map[string]any{
			"ok":            true,
			"connected":     robot.Connected,
			"ip":            robot.IP,
			"goal_active":   a.status.GoalActive,
			"goal_target":   a.status.GoalTarget,
			"last_action":   a.status.LastAction,
			"orientation":   orientationData,
			"agent_running": a.status.Running,
		}), nil
	}

	return "", fmt.Errorf("unsupported tool: %s", call.Name)
}

func (a *realtimeAgent) nextImageDataURLLocked(force bool) (string, error) {
	now := time.Now()
	if !force && !a.lastFrameSentAt.IsZero() && now.Sub(a.lastFrameSentAt) < agentImageMinGap {
		return "", nil
	}

	var frame videoFrame
	if err := a.worker.call("video_frame", nil, &frame); err != nil {
		return "", err
	}
	if strings.TrimSpace(frame.ImageB64) == "" {
		return "", nil
	}

	a.lastFrameSentAt = now
	return "data:image/jpeg;base64," + frame.ImageB64, nil
}

func parseErrorMessage(event map[string]any) string {
	if em, ok := event["error"].(map[string]any); ok {
		if msg, ok := em["message"].(string); ok && strings.TrimSpace(msg) != "" {
			return strings.TrimSpace(msg)
		}
	}
	if msg, ok := event["message"].(string); ok && strings.TrimSpace(msg) != "" {
		return strings.TrimSpace(msg)
	}
	return "realtime error"
}

func parseResponseDone(event map[string]any) (string, []toolCall) {
	response, _ := event["response"].(map[string]any)
	output, _ := response["output"].([]any)

	toolCalls := make([]toolCall, 0)
	textParts := make([]string, 0)

	for _, item := range output {
		m, ok := item.(map[string]any)
		if !ok {
			continue
		}

		itemType, _ := m["type"].(string)
		switch itemType {
		case "function_call":
			name, _ := m["name"].(string)
			callID, _ := m["call_id"].(string)
			args, _ := m["arguments"].(string)
			if name != "" && callID != "" {
				toolCalls = append(toolCalls, toolCall{Name: name, CallID: callID, Arguments: args})
			}
		case "message":
			content, _ := m["content"].([]any)
			for _, c := range content {
				part, ok := c.(map[string]any)
				if !ok {
					continue
				}
				if text, ok := part["text"].(string); ok && strings.TrimSpace(text) != "" {
					textParts = append(textParts, strings.TrimSpace(text))
				}
				if text, ok := part["transcript"].(string); ok && strings.TrimSpace(text) != "" {
					textParts = append(textParts, strings.TrimSpace(text))
				}
			}
		}
	}

	return strings.TrimSpace(strings.Join(textParts, "\n")), toolCalls
}

func (a *realtimeAgent) runTurnLocked(prompt string, includeImage bool, forceImage bool) (string, error) {
	if a.conn == nil || !a.status.Running {
		return "", errors.New("agent is not running")
	}
	if a.eventCh == nil || a.readErrCh == nil {
		return "", errors.New("agent realtime stream is not ready")
	}

	prompt = strings.TrimSpace(prompt)
	if prompt == "" {
		return "", errors.New("input text is required")
	}
	a.drainEventsLocked()

	content := make([]map[string]any, 0, 2)
	content = append(content, map[string]any{
		"type": "input_text",
		"text": prompt,
	})

	if includeImage {
		img, err := a.nextImageDataURLLocked(forceImage)
		if err != nil {
			a.status.LastError = fmt.Sprintf("frame fetch failed: %v", err)
			a.markUpdatedLocked()
		} else if img != "" {
			content = append(content, map[string]any{
				"type":      "input_image",
				"image_url": img,
			})
		}
	}

	msgEvent := map[string]any{
		"type": "conversation.item.create",
		"item": map[string]any{
			"type":    "message",
			"role":    "user",
			"content": content,
		},
	}
	if err := a.writeJSONLocked(msgEvent); err != nil {
		a.resetLocked()
		return "", fmt.Errorf("send conversation item failed: %w", err)
	}

	createEvent := map[string]any{
		"type": "response.create",
		"response": map[string]any{
			"output_modalities": []string{"text"},
		},
	}
	if err := a.writeJSONLocked(createEvent); err != nil {
		a.resetLocked()
		return "", fmt.Errorf("send response.create failed: %w", err)
	}

	var reply strings.Builder
	timer := time.NewTimer(agentAskTimeout)
	defer timer.Stop()
	resetTimer := func() {
		if !timer.Stop() {
			select {
			case <-timer.C:
			default:
			}
		}
		timer.Reset(agentAskTimeout)
	}

	for {
		var event map[string]any
		select {
		case <-timer.C:
			return "", errors.New("realtime response timeout")
		case err := <-a.readErrCh:
			a.resetLocked()
			if err == nil {
				return "", errors.New("realtime read failed: connection closed")
			}
			return "", fmt.Errorf("realtime read failed: %w", err)
		case evt, ok := <-a.eventCh:
			if !ok {
				a.resetLocked()
				return "", errors.New("realtime read failed: connection closed")
			}
			event = evt
		}

		evtType, _ := event["type"].(string)
		switch evtType {
		case "error":
			return "", errors.New(parseErrorMessage(event))

		case "response.output_text.delta":
			if delta, ok := event["delta"].(string); ok {
				reply.WriteString(delta)
			}

		case "response.done":
			doneText, calls := parseResponseDone(event)
			if doneText != "" {
				if reply.Len() > 0 {
					reply.WriteByte('\n')
				}
				reply.WriteString(doneText)
			}

			if len(calls) > 0 {
				for _, call := range calls {
					output, toolErr := a.executeToolLocked(call)
					if toolErr != nil {
						output = mustJSON(map[string]any{
							"ok":    false,
							"error": toolErr.Error(),
						})
						a.status.LastError = toolErr.Error()
					}

					outputEvent := map[string]any{
						"type": "conversation.item.create",
						"item": map[string]any{
							"type":    "function_call_output",
							"call_id": call.CallID,
							"output":  output,
						},
					}
					if err := a.writeJSONLocked(outputEvent); err != nil {
						a.resetLocked()
						return "", fmt.Errorf("send function_call_output failed: %w", err)
					}
				}

				if err := a.writeJSONLocked(createEvent); err != nil {
					a.resetLocked()
					return "", fmt.Errorf("send follow-up response.create failed: %w", err)
				}
				resetTimer()
				continue
			}

			final := strings.TrimSpace(reply.String())
			a.status.LastReply = final
			a.status.LastError = ""
			a.markUpdatedLocked()
			return final, nil
		}
	}
}

func (a *realtimeAgent) goalLoop(ctx context.Context, target string) {
	ticker := time.NewTicker(goalTickInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			prompt := fmt.Sprintf(
				"Autonomous object goal is active. Target object: %q.\n"+
					"Analyze the latest image and decide the next safe robot action.\n"+
					"Always use tools only:\n"+
					"- call robot_command for incremental movement,\n"+
					"- call robot_rotate for controlled angle turns,\n"+
					"- call stop_object_goal when target is reached or cannot be safely approached. "+
					"(An object is reached, when the object touches the center of the lower edge of "+
					"the picture. "+
					"And tell, when it is reached, or not reachable!)\n"+
					"Avoid obstacles by choosing alternative paths.",
				//"Prefer short, safe steps and avoid obstacles.",
				target,
			)

			a.mu.Lock()
			if !a.status.Running || !a.status.GoalActive || a.status.GoalTarget != target {
				a.mu.Unlock()
				return
			}

			_, err := a.runTurnLocked(prompt, true, true)
			if err != nil {
				a.status.LastError = err.Error()
				a.stopGoalLocked(true)
				a.markUpdatedLocked()
				a.mu.Unlock()
				return
			}
			a.mu.Unlock()
		}
	}
}

func (a *realtimeAgent) SendUserInput(text string) (string, error) {
	a.mu.Lock()
	defer a.mu.Unlock()

	if !a.status.Running {
		return "", errors.New("agent is not running")
	}

	a.status.LastInputAt = time.Now().Format(time.RFC3339)
	reply, err := a.runTurnLocked(text, true, false)
	if err != nil {
		a.status.LastError = err.Error()
		a.markUpdatedLocked()
		return "", err
	}
	a.markUpdatedLocked()
	return reply, nil
}

func mustJSON(v any) string {
	data, err := json.Marshal(v)
	if err != nil {
		return "{\"ok\":false,\"error\":\"json encode failed\"}"
	}
	return string(data)
}

type apiResponse struct {
	OK          bool               `json:"ok"`
	Error       string             `json:"error,omitempty"`
	Message     string             `json:"message,omitempty"`
	Status      *workerStatus      `json:"status,omitempty"`
	Video       *videoFrame        `json:"video,omitempty"`
	Orientation *workerOrientation `json:"orientation,omitempty"`
	Agent       *agentStatus       `json:"agent,omitempty"`
	Reply       string             `json:"reply,omitempty"`
}

type connectRequest struct {
	IP     string `json:"ip"`
	AESKey string `json:"aesKey"`
}

type agentStartRequest struct {
	APIKey string `json:"apiKey"`
}

type textRequest struct {
	Text string `json:"text"`
}

type appServer struct {
	worker *workerClient
	agent  *realtimeAgent
}

func (s *appServer) snapshotStatus() (workerStatus, agentStatus) {
	var robot workerStatus
	_ = s.worker.call("status", nil, &robot)
	return robot, s.agent.snapshot()
}

func (s *appServer) handleStatus(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeJSON(w, http.StatusMethodNotAllowed, apiResponse{OK: false, Error: "method not allowed"})
		return
	}

	var robot workerStatus
	if err := s.worker.call("status", nil, &robot); err != nil {
		writeJSON(w, http.StatusInternalServerError, apiResponse{OK: false, Error: err.Error()})
		return
	}

	agent := s.agent.snapshot()
	writeJSON(w, http.StatusOK, apiResponse{OK: true, Status: &robot, Agent: &agent})
}

func (s *appServer) handleConnect(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeJSON(w, http.StatusMethodNotAllowed, apiResponse{OK: false, Error: "method not allowed"})
		return
	}

	var req connectRequest
	if err := decodeJSON(r, &req); err != nil {
		writeJSON(w, http.StatusBadRequest, apiResponse{OK: false, Error: err.Error()})
		return
	}

	ip := strings.TrimSpace(req.IP)
	if ip == "" {
		ip = defaultRobotIP
	}

	// Force a clean agent session whenever robot transport changes.
	s.agent.Stop()

	var status workerStatus
	if err := s.worker.call("connect", map[string]string{"ip": ip, "aes_key": strings.TrimSpace(req.AESKey)}, &status); err != nil {
		writeJSON(w, http.StatusBadRequest, apiResponse{OK: false, Error: err.Error()})
		return
	}

	agent := s.agent.snapshot()
	writeJSON(w, http.StatusOK, apiResponse{OK: true, Status: &status, Agent: &agent})
}

func (s *appServer) handleDisconnect(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeJSON(w, http.StatusMethodNotAllowed, apiResponse{OK: false, Error: "method not allowed"})
		return
	}

	s.agent.Stop()

	var status workerStatus
	if err := s.worker.call("disconnect", nil, &status); err != nil {
		writeJSON(w, http.StatusInternalServerError, apiResponse{OK: false, Error: err.Error()})
		return
	}

	agent := s.agent.snapshot()
	writeJSON(w, http.StatusOK, apiResponse{OK: true, Status: &status, Agent: &agent})
}

func (s *appServer) handleVideoFrame(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeJSON(w, http.StatusMethodNotAllowed, apiResponse{OK: false, Error: "method not allowed"})
		return
	}

	var frame videoFrame
	if err := s.worker.call("video_frame", nil, &frame); err != nil {
		writeJSON(w, http.StatusBadRequest, apiResponse{OK: false, Error: err.Error()})
		return
	}

	writeJSON(w, http.StatusOK, apiResponse{OK: true, Video: &frame})
}

func (s *appServer) handleOrientation(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeJSON(w, http.StatusMethodNotAllowed, apiResponse{OK: false, Error: "method not allowed"})
		return
	}

	var orientation workerOrientation
	if err := s.worker.call("orientation", nil, &orientation); err != nil {
		writeJSON(w, http.StatusBadRequest, apiResponse{OK: false, Error: err.Error()})
		return
	}

	writeJSON(w, http.StatusOK, apiResponse{OK: true, Orientation: &orientation})
}

func (s *appServer) handleAgentStart(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeJSON(w, http.StatusMethodNotAllowed, apiResponse{OK: false, Error: "method not allowed"})
		return
	}

	var req agentStartRequest
	if err := decodeJSON(r, &req); err != nil {
		writeJSON(w, http.StatusBadRequest, apiResponse{OK: false, Error: err.Error()})
		return
	}

	var robot workerStatus
	if err := s.worker.call("status", nil, &robot); err != nil {
		writeJSON(w, http.StatusInternalServerError, apiResponse{OK: false, Error: err.Error()})
		return
	}
	if !robot.Connected {
		writeJSON(w, http.StatusBadRequest, apiResponse{OK: false, Error: "robot is not connected"})
		return
	}

	if err := s.agent.Start(req.APIKey); err != nil {
		writeJSON(w, http.StatusBadGateway, apiResponse{OK: false, Error: err.Error()})
		return
	}

	robot, agent := s.snapshotStatus()
	writeJSON(w, http.StatusOK, apiResponse{OK: true, Message: "agent started", Status: &robot, Agent: &agent})
}

func (s *appServer) handleAgentStop(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeJSON(w, http.StatusMethodNotAllowed, apiResponse{OK: false, Error: "method not allowed"})
		return
	}

	s.agent.Stop()
	robot, agent := s.snapshotStatus()
	writeJSON(w, http.StatusOK, apiResponse{OK: true, Message: "agent stopped", Status: &robot, Agent: &agent})
}

func (s *appServer) handleAgentMessage(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeJSON(w, http.StatusMethodNotAllowed, apiResponse{OK: false, Error: "method not allowed"})
		return
	}

	var req textRequest
	if err := decodeJSON(r, &req); err != nil {
		writeJSON(w, http.StatusBadRequest, apiResponse{OK: false, Error: err.Error()})
		return
	}

	text := strings.TrimSpace(req.Text)
	if text == "" {
		writeJSON(w, http.StatusBadRequest, apiResponse{OK: false, Error: "text is required"})
		return
	}

	reply, err := s.agent.SendUserInput(text)
	if err != nil {
		writeJSON(w, http.StatusBadGateway, apiResponse{OK: false, Error: err.Error()})
		return
	}

	robot, agent := s.snapshotStatus()
	writeJSON(w, http.StatusOK, apiResponse{
		OK:      true,
		Message: "agent response",
		Reply:   reply,
		Status:  &robot,
		Agent:   &agent,
	})
}

func decodeJSON(r *http.Request, target any) error {
	defer r.Body.Close()
	dec := json.NewDecoder(r.Body)
	dec.DisallowUnknownFields()
	if err := dec.Decode(target); err != nil {
		return fmt.Errorf("invalid json body: %w", err)
	}

	var extra struct{}
	if err := dec.Decode(&extra); err != io.EOF {
		return errors.New("invalid json body: trailing data")
	}
	return nil
}

func writeJSON(w http.ResponseWriter, code int, payload any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(payload)
}

func joinPrefix(prefix, path string) string {
	p := strings.TrimSpace(prefix)
	if p == "" {
		return path
	}
	if !strings.HasPrefix(p, "/") {
		p = "/" + p
	}
	p = strings.TrimSuffix(p, "/")
	return p + path
}

func registerRoutes(mux *http.ServeMux, server *appServer, webDir, prefix string) {
	mux.HandleFunc(joinPrefix(prefix, "/api/status"), server.handleStatus)
	mux.HandleFunc(joinPrefix(prefix, "/api/connect"), server.handleConnect)
	mux.HandleFunc(joinPrefix(prefix, "/api/disconnect"), server.handleDisconnect)
	mux.HandleFunc(joinPrefix(prefix, "/api/video/frame"), server.handleVideoFrame)
	mux.HandleFunc(joinPrefix(prefix, "/api/orientation"), server.handleOrientation)
	mux.HandleFunc(joinPrefix(prefix, "/api/agent/start"), server.handleAgentStart)
	mux.HandleFunc(joinPrefix(prefix, "/api/agent/stop"), server.handleAgentStop)
	mux.HandleFunc(joinPrefix(prefix, "/api/agent/message"), server.handleAgentMessage)

	webBase := joinPrefix(prefix, "/web/")
	fileHandler := http.StripPrefix(webBase, http.FileServer(http.Dir(webDir)))
	mux.Handle(webBase, fileHandler)

	mux.HandleFunc(strings.TrimSuffix(webBase, "/"), func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, webBase, http.StatusMovedPermanently)
	})

	if prefix == "" {
		mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
			if r.URL.Path != "/" {
				http.NotFound(w, r)
				return
			}
			http.Redirect(w, r, "/web/", http.StatusFound)
		})
	} else {
		p := strings.TrimSuffix(prefix, "/")
		mux.HandleFunc(p, func(w http.ResponseWriter, r *http.Request) {
			if r.URL.Path != p {
				http.NotFound(w, r)
				return
			}
			http.Redirect(w, r, webBase, http.StatusFound)
		})
		mux.HandleFunc(p+"/", func(w http.ResponseWriter, r *http.Request) {
			if r.URL.Path != p+"/" {
				http.NotFound(w, r)
				return
			}
			http.Redirect(w, r, webBase, http.StatusFound)
		})
	}
}

func main() {
	if runtime.GOOS == "windows" {
		log.Fatal(
			"This server is configured to run from WSL/Linux only. " +
				"Refusing to start on native Windows to avoid a second server instance.",
		)
	}

	workerScript := filepath.Join("worker", "unitree_worker.py")
	if _, err := os.Stat(workerScript); err != nil {
		log.Fatalf("worker script not found at %s: %v", workerScript, err)
	}

	worker := newWorkerClient(workerScript)
	defer worker.close()

	var startupStatus workerStatus
	if err := worker.call("status", nil, &startupStatus); err != nil {
		log.Fatalf("failed to start worker: %v", err)
	}

	agent := newRealtimeAgent(worker)
	defer agent.Stop()

	server := &appServer{
		worker: worker,
		agent:  agent,
	}

	mux := http.NewServeMux()
	webDir := filepath.Join(".", "web")
	registerRoutes(mux, server, webDir, "")
	registerRoutes(mux, server, webDir, "/robot")

	port := strings.TrimSpace(os.Getenv("PORT"))
	if port == "" {
		port = defaultPort
	}

	addr := ":" + port
	log.Printf("Server listening on http://localhost%s", addr)
	log.Printf("Robot default IP: %s", defaultRobotIP)
	log.Printf("OpenAI realtime model: %s", chooseModel())
	log.Printf("UI: http://localhost%s/web/ | API: http://localhost%s/api/*", addr, addr)
	log.Printf("UI: http://localhost%s/robot/web/ | API: http://localhost%s/robot/api/*", addr, addr)

	if err := http.ListenAndServe(addr, mux); err != nil {
		log.Fatal(err)
	}
}
