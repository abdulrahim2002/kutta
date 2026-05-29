package main

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"
)

const (
	defaultRobotIP      = "10.174.16.11"
	defaultPort         = "8080"
	defaultGeminiModel  = "gemini-3.1-flash-live"
	agentTickInterval   = 850 * time.Millisecond
	agentRecoverTurnFor = 1200 * time.Millisecond
	agentScanTurnFor    = 2400 * time.Millisecond
	agentSearchStepFor  = 900 * time.Millisecond
)

var allowedCommands = map[string]struct{}{
	"forward":  {},
	"backward": {},
	"left":     {},
	"right":    {},
	"stop":     {},
	"laydown":  {},
	"standup":  {},
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
	return &workerClient{
		scriptPath: scriptPath,
		pythonBin:  pythonBin,
	}
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
	req := workerRequest{
		ID:     c.nextID,
		Action: action,
		Params: params,
	}

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

type geminiClient struct {
	apiKey     string
	model      string
	httpClient *http.Client
}

func newGeminiClient() *geminiClient {
	model := strings.TrimSpace(os.Getenv("GEMINI_MODEL"))
	if model == "" {
		model = defaultGeminiModel
	}

	return &geminiClient{
		apiKey: strings.TrimSpace(os.Getenv("GEMINI_API_KEY")),
		model:  model,
		httpClient: &http.Client{
			Timeout: 12 * time.Second,
		},
	}
}

func (g *geminiClient) enabled() bool {
	return g != nil && g.apiKey != ""
}

func (g *geminiClient) modelPath() string {
	model := strings.TrimSpace(g.model)
	if strings.HasPrefix(model, "models/") {
		return model
	}
	return "models/" + model
}

type geminiResponse struct {
	Candidates []struct {
		Content struct {
			Parts []struct {
				Text string `json:"text"`
			} `json:"parts"`
		} `json:"content"`
	} `json:"candidates"`
	Error *struct {
		Message string `json:"message"`
	} `json:"error"`
}

func (g *geminiClient) generateJSON(ctx context.Context, prompt, imageB64 string, out any) error {
	if !g.enabled() {
		return errors.New("gemini api key is not configured")
	}

	parts := []map[string]any{
		{"text": prompt},
	}
	if strings.TrimSpace(imageB64) != "" {
		parts = append(parts, map[string]any{
			"inline_data": map[string]any{
				"mime_type": "image/jpeg",
				"data":      imageB64,
			},
		})
	}

	body := map[string]any{
		"contents": []map[string]any{
			{
				"role":  "user",
				"parts": parts,
			},
		},
		"generationConfig": map[string]any{
			"temperature":      0.1,
			"maxOutputTokens":  256,
			"responseMimeType": "application/json",
		},
	}

	rawBody, err := json.Marshal(body)
	if err != nil {
		return fmt.Errorf("failed to encode gemini request: %w", err)
	}

	endpoint := fmt.Sprintf(
		"https://generativelanguage.googleapis.com/v1beta/%s:generateContent?key=%s",
		g.modelPath(),
		url.QueryEscape(g.apiKey),
	)

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint, bytes.NewReader(rawBody))
	if err != nil {
		return fmt.Errorf("failed to build gemini request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := g.httpClient.Do(req)
	if err != nil {
		return fmt.Errorf("gemini request failed: %w", err)
	}
	defer resp.Body.Close()

	payload, err := io.ReadAll(resp.Body)
	if err != nil {
		return fmt.Errorf("failed to read gemini response: %w", err)
	}

	var parsed geminiResponse
	if err := json.Unmarshal(payload, &parsed); err != nil {
		return fmt.Errorf("failed to decode gemini response: %w", err)
	}

	if resp.StatusCode >= 400 {
		if parsed.Error != nil && parsed.Error.Message != "" {
			return fmt.Errorf("gemini http %d: %s", resp.StatusCode, parsed.Error.Message)
		}
		return fmt.Errorf("gemini http %d", resp.StatusCode)
	}

	if parsed.Error != nil && parsed.Error.Message != "" {
		return errors.New(parsed.Error.Message)
	}

	text := ""
	for _, candidate := range parsed.Candidates {
		for _, part := range candidate.Content.Parts {
			if strings.TrimSpace(part.Text) != "" {
				text = part.Text
				break
			}
		}
		if text != "" {
			break
		}
	}
	if text == "" {
		return errors.New("gemini returned no text payload")
	}

	clean := extractJSONObject(text)
	if err := json.Unmarshal([]byte(clean), out); err != nil {
		return fmt.Errorf("gemini returned invalid json: %w; raw=%s", err, clean)
	}
	return nil
}

func extractJSONObject(raw string) string {
	s := strings.TrimSpace(raw)
	s = strings.TrimPrefix(s, "```json")
	s = strings.TrimPrefix(s, "```")
	s = strings.TrimSuffix(s, "```")
	s = strings.TrimSpace(s)
	start := strings.Index(s, "{")
	end := strings.LastIndex(s, "}")
	if start >= 0 && end > start {
		return s[start : end+1]
	}
	return s
}

type voiceAction struct {
	Type    string `json:"type"`
	Command string `json:"command,omitempty"`
	Target  string `json:"target,omitempty"`
	Reason  string `json:"reason,omitempty"`
}

func normalizeCommand(command string) string {
	cmd := strings.ToLower(strings.TrimSpace(command))
	if _, ok := allowedCommands[cmd]; ok {
		return cmd
	}
	return ""
}

func normalizeVoiceAction(action voiceAction) voiceAction {
	action.Type = strings.ToLower(strings.TrimSpace(action.Type))
	action.Command = normalizeCommand(action.Command)
	action.Target = strings.ToLower(strings.TrimSpace(action.Target))
	action.Reason = strings.TrimSpace(action.Reason)

	switch action.Type {
	case "move":
		if action.Command == "" {
			action.Type = "noop"
		}
	case "agent_start":
		if action.Target == "" {
			action.Type = "noop"
		}
	case "agent_stop":
	default:
		action.Type = "noop"
	}
	return action
}

func (g *geminiClient) parseVoiceCommand(ctx context.Context, raw string) (voiceAction, string, error) {
	if !g.enabled() {
		return voiceAction{}, "", errors.New("GEMINI_API_KEY is missing")
	}

	prompt := fmt.Sprintf(
		"Convert this robot voice command into JSON.\n"+
			"Transcript: %q\n"+
			"Return exactly one JSON object with schema:\n"+
			"{\"type\":\"move|agent_start|agent_stop|noop\",\"command\":\"forward|backward|left|right|stop|laydown\",\"target\":\"string\",\"reason\":\"string\"}\n"+
			"Rules:\n"+
			"- Direct movement requests -> type=move.\n"+
			"- Object goals like 'go to the bottle' -> type=agent_start and target='bottle'.\n"+
			"- Stop autonomous behavior -> type=agent_stop.\n"+
			"- Unknown request -> type=noop.\n"+
			"Return JSON only.",
		raw,
	)

	var action voiceAction
	if err := g.generateJSON(ctx, prompt, "", &action); err != nil {
		return voiceAction{}, "", fmt.Errorf("gemini voice parsing failed: %w", err)
	}
	return normalizeVoiceAction(action), "gemini", nil
}

type navigationDecision struct {
	Action        string  `json:"action"`
	TargetVisible bool    `json:"target_visible"`
	ObstacleAhead bool    `json:"obstacle_ahead"`
	Confidence    float64 `json:"confidence"`
	Reason        string  `json:"reason"`
}

func normalizeNavigationAction(raw string) string {
	switch strings.ToLower(strings.TrimSpace(raw)) {
	case "forward", "backward", "left", "right", "stop", "scan":
		return strings.ToLower(strings.TrimSpace(raw))
	default:
		return "scan"
	}
}

func (g *geminiClient) decideNavigation(ctx context.Context, target, imageB64, lastMotion string) (navigationDecision, error) {
	if strings.TrimSpace(imageB64) == "" {
		return navigationDecision{}, errors.New("no camera frame available for gemini navigation")
	}
	if !g.enabled() {
		return navigationDecision{}, errors.New("GEMINI_API_KEY is missing")
	}

	prompt := fmt.Sprintf(
		"You are a low-latency navigation policy for a quadruped robot.\n"+
			"Goal object: %q\n"+
			"Previous robot motion command: %q\n"+
			"From the current camera frame, return one JSON object:\n"+
			"{\"action\":\"forward|left|right|stop|scan\",\"target_visible\":bool,\"obstacle_ahead\":bool,\"confidence\":0..1,\"reason\":\"short\"}\n"+
			"Policy:\n"+
			"- If target not visible, scan by turning left or right.\n"+
			"- If target visible and path appears clear, action=forward.\n"+
			"- If immediate obstacle risk exists, obstacle_ahead=true and action left or right.\n"+
			"- Be conservative around obstacles.\n"+
			"Return JSON only.",
		target,
		lastMotion,
	)

	var decision navigationDecision
	if err := g.generateJSON(ctx, prompt, imageB64, &decision); err != nil {
		return navigationDecision{}, fmt.Errorf("gemini navigation failed: %w", err)
	}
	decision.Action = normalizeNavigationAction(decision.Action)
	if decision.Confidence < 0 {
		decision.Confidence = 0
	}
	if decision.Confidence > 1 {
		decision.Confidence = 1
	}
	return decision, nil
}

type agentStatus struct {
	Running    bool   `json:"running"`
	Target     string `json:"target,omitempty"`
	LastAction string `json:"last_action,omitempty"`
	LastReason string `json:"last_reason,omitempty"`
	LastError  string `json:"last_error,omitempty"`
	Iteration  int    `json:"iteration,omitempty"`
	UpdatedAt  string `json:"updated_at,omitempty"`
}

type agentController struct {
	worker *workerClient
	gemini *geminiClient

	mu     sync.Mutex
	status agentStatus
	cancel context.CancelFunc
	runID  int64
}

func newAgentController(worker *workerClient, gemini *geminiClient) *agentController {
	return &agentController{
		worker: worker,
		gemini: gemini,
		status: agentStatus{
			Running:    false,
			LastAction: "stop",
			LastReason: "idle",
			UpdatedAt:  time.Now().Format(time.RFC3339),
		},
	}
}

func (a *agentController) snapshot() agentStatus {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.status
}

func (a *agentController) updateIfCurrent(runID int64, mut func(s *agentStatus)) {
	a.mu.Lock()
	defer a.mu.Unlock()
	if runID != a.runID || !a.status.Running {
		return
	}
	mut(&a.status)
	a.status.UpdatedAt = time.Now().Format(time.RFC3339)
}

func (a *agentController) Start(target string) error {
	target = strings.TrimSpace(target)
	if target == "" {
		return errors.New("target is required")
	}
	if !a.gemini.enabled() {
		return errors.New("GEMINI_API_KEY is required for agent mode")
	}

	a.mu.Lock()
	if a.cancel != nil {
		a.cancel()
	}
	ctx, cancel := context.WithCancel(context.Background())
	a.cancel = cancel
	a.runID++
	runID := a.runID
	a.status = agentStatus{
		Running:    true,
		Target:     target,
		LastAction: "scan",
		LastReason: "agent started",
		UpdatedAt:  time.Now().Format(time.RFC3339),
	}
	a.mu.Unlock()

	go a.run(ctx, runID, target)
	return nil
}

func (a *agentController) Stop() {
	a.mu.Lock()
	cancel := a.cancel
	wasRunning := a.status.Running
	a.runID++
	a.cancel = nil
	a.status = agentStatus{
		Running:    false,
		LastAction: "stop",
		LastReason: "agent stopped",
		UpdatedAt:  time.Now().Format(time.RFC3339),
	}
	a.mu.Unlock()

	if cancel != nil {
		cancel()
	}
	if wasRunning {
		_ = a.worker.call("command", map[string]string{"command": "stop"}, nil)
	}
}

func (a *agentController) run(ctx context.Context, runID int64, target string) {
	ticker := time.NewTicker(agentTickInterval)
	defer ticker.Stop()

	lastMotion := "stop"
	var recoveryUntil time.Time
	recoveryTurn := ""
	scanDirection := "left"
	scanTurnUntil := time.Now().Add(agentScanTurnFor)
	scanHalfSweeps := 0
	var searchForwardUntil time.Time

	for {
		select {
		case <-ctx.Done():
			_ = a.worker.call("command", map[string]string{"command": "stop"}, nil)
			return
		case <-ticker.C:
			command, reason, err := a.nextCommand(
				ctx,
				target,
				lastMotion,
				&recoveryUntil,
				&recoveryTurn,
				&scanDirection,
				&scanTurnUntil,
				&scanHalfSweeps,
				&searchForwardUntil,
			)
			if err != nil {
				_ = a.worker.call("command", map[string]string{"command": "stop"}, nil)
				a.updateIfCurrent(runID, func(s *agentStatus) {
					s.LastError = err.Error()
					s.LastReason = "decision error"
					s.LastAction = "stop"
				})
				continue
			}

			if err := a.worker.call("command", map[string]string{"command": command}, nil); err != nil {
				a.updateIfCurrent(runID, func(s *agentStatus) {
					s.LastError = err.Error()
					s.LastReason = "command send error"
				})
				continue
			}

			lastMotion = command
			a.updateIfCurrent(runID, func(s *agentStatus) {
				s.Iteration++
				s.LastAction = command
				s.LastReason = reason
				s.LastError = ""
			})
		}
	}
}

func chooseTurnFromLast(last string) string {
	if strings.EqualFold(last, "left") {
		return "right"
	}
	return "left"
}

func chooseSearchCommand(
	now time.Time,
	scanDirection *string,
	scanTurnUntil *time.Time,
	scanHalfSweeps *int,
	searchForwardUntil *time.Time,
) (string, string) {
	if now.Before(*searchForwardUntil) {
		return "forward", "search probe forward"
	}

	dir := strings.ToLower(strings.TrimSpace(*scanDirection))
	if dir != "left" && dir != "right" {
		dir = "left"
	}

	if now.After(*scanTurnUntil) {
		if dir == "left" {
			dir = "right"
		} else {
			dir = "left"
		}
		*scanTurnUntil = now.Add(agentScanTurnFor)
		*scanHalfSweeps++

		// After each full left+right sweep, do a short forward probe.
		if *scanHalfSweeps%2 == 0 {
			*searchForwardUntil = now.Add(agentSearchStepFor)
			*scanDirection = dir
			return "forward", "target not visible, short forward probe"
		}
	}

	*scanDirection = dir
	return dir, "target not visible, scanning"
}

func (a *agentController) nextCommand(
	ctx context.Context,
	target string,
	lastMotion string,
	recoveryUntil *time.Time,
	recoveryTurn *string,
	scanDirection *string,
	scanTurnUntil *time.Time,
	scanHalfSweeps *int,
	searchForwardUntil *time.Time,
) (string, string, error) {
	now := time.Now()
	if now.Before(*recoveryUntil) && *recoveryTurn != "" {
		return *recoveryTurn, "temporary recovery turn", nil
	}

	var frame videoFrame
	if err := a.worker.call("video_frame", nil, &frame); err != nil {
		command, reason := chooseSearchCommand(
			now,
			scanDirection,
			scanTurnUntil,
			scanHalfSweeps,
			searchForwardUntil,
		)
		return command, "frame unavailable, " + reason, nil
	}

	decisionCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()

	decision, err := a.gemini.decideNavigation(decisionCtx, target, frame.ImageB64, lastMotion)
	if err != nil {
		return "", "", err
	}

	command := normalizeNavigationAction(decision.Action)
	if command == "scan" {
		command = chooseTurnFromLast(lastMotion)
	}
	command = normalizeCommand(command)
	if command == "" {
		command = chooseTurnFromLast(lastMotion)
	}

	if decision.ObstacleAhead {
		*recoveryTurn = chooseTurnFromLast(lastMotion)
		*recoveryUntil = now.Add(agentRecoverTurnFor)
		*searchForwardUntil = time.Time{}
		return *recoveryTurn, "obstacle ahead, recovery turn", nil
	}

	if !decision.TargetVisible {
		scanCommand, scanReason := chooseSearchCommand(
			now,
			scanDirection,
			scanTurnUntil,
			scanHalfSweeps,
			searchForwardUntil,
		)
		return scanCommand, scanReason, nil
	}

	// Target found: reset scan counters and move toward target.
	*scanHalfSweeps = 0
	*searchForwardUntil = time.Time{}

	if command == "scan" || command == "stop" {
		command = "forward"
	}
	if command == "backward" {
		command = chooseTurnFromLast(lastMotion)
	}

	if command == "" {
		command = "forward"
	}
	reason := strings.TrimSpace(decision.Reason)
	if reason == "" {
		reason = "navigation decision"
	}
	return command, reason, nil
}

type apiResponse struct {
	OK           bool          `json:"ok"`
	Error        string        `json:"error,omitempty"`
	Message      string        `json:"message,omitempty"`
	Status       *workerStatus `json:"status,omitempty"`
	Video        *videoFrame   `json:"video,omitempty"`
	Action       *voiceAction  `json:"action,omitempty"`
	Agent        *agentStatus  `json:"agent,omitempty"`
	GeminiModel  string        `json:"gemini_model,omitempty"`
	GeminiActive bool          `json:"gemini_active,omitempty"`
	Source       string        `json:"source,omitempty"`
}

type connectRequest struct {
	IP     string `json:"ip"`
	AESKey string `json:"aesKey"`
}

type commandRequest struct {
	Command string `json:"command"`
}

type voiceRequest struct {
	Text string `json:"text"`
}

type agentStartRequest struct {
	Target string `json:"target"`
}

type appServer struct {
	worker *workerClient
	gemini *geminiClient
	agent  *agentController
}

func (s *appServer) handleStatus(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeJSON(w, http.StatusMethodNotAllowed, apiResponse{OK: false, Error: "method not allowed"})
		return
	}

	var status workerStatus
	if err := s.worker.call("status", nil, &status); err != nil {
		writeJSON(w, http.StatusInternalServerError, apiResponse{OK: false, Error: err.Error()})
		return
	}

	agentStatus := s.agent.snapshot()
	writeJSON(w, http.StatusOK, apiResponse{
		OK:           true,
		Status:       &status,
		Agent:        &agentStatus,
		GeminiModel:  s.gemini.model,
		GeminiActive: s.gemini.enabled(),
	})
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

	params := map[string]string{
		"ip":      ip,
		"aes_key": strings.TrimSpace(req.AESKey),
	}

	var status workerStatus
	if err := s.worker.call("connect", params, &status); err != nil {
		writeJSON(w, http.StatusBadRequest, apiResponse{OK: false, Error: err.Error()})
		return
	}

	agentStatus := s.agent.snapshot()
	writeJSON(w, http.StatusOK, apiResponse{OK: true, Status: &status, Agent: &agentStatus})
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

	agentStatus := s.agent.snapshot()
	writeJSON(w, http.StatusOK, apiResponse{OK: true, Status: &status, Agent: &agentStatus})
}

func (s *appServer) handleCommand(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeJSON(w, http.StatusMethodNotAllowed, apiResponse{OK: false, Error: "method not allowed"})
		return
	}

	var req commandRequest
	if err := decodeJSON(r, &req); err != nil {
		writeJSON(w, http.StatusBadRequest, apiResponse{OK: false, Error: err.Error()})
		return
	}

	command := normalizeCommand(req.Command)
	if command == "" {
		writeJSON(w, http.StatusBadRequest, apiResponse{OK: false, Error: "unsupported command"})
		return
	}

	// Manual command overrides autonomous navigation.
	s.agent.Stop()

	if err := s.worker.call("command", map[string]string{"command": command}, nil); err != nil {
		writeJSON(w, http.StatusBadRequest, apiResponse{OK: false, Error: err.Error()})
		return
	}

	var status workerStatus
	_ = s.worker.call("status", nil, &status)
	agentStatus := s.agent.snapshot()
	writeJSON(w, http.StatusOK, apiResponse{
		OK:      true,
		Status:  &status,
		Agent:   &agentStatus,
		Message: "command sent",
	})
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

func (s *appServer) handleVoiceCommand(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeJSON(w, http.StatusMethodNotAllowed, apiResponse{OK: false, Error: "method not allowed"})
		return
	}

	var req voiceRequest
	if err := decodeJSON(r, &req); err != nil {
		writeJSON(w, http.StatusBadRequest, apiResponse{OK: false, Error: err.Error()})
		return
	}

	text := strings.TrimSpace(req.Text)
	if text == "" {
		writeJSON(w, http.StatusBadRequest, apiResponse{OK: false, Error: "text is required"})
		return
	}

	ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
	defer cancel()
	action, source, err := s.gemini.parseVoiceCommand(ctx, text)
	if err != nil {
		writeJSON(w, http.StatusBadGateway, apiResponse{
			OK:    false,
			Error: err.Error(),
		})
		return
	}
	action = normalizeVoiceAction(action)

	switch action.Type {
	case "move":
		s.agent.Stop()
		if err := s.worker.call("command", map[string]string{"command": action.Command}, nil); err != nil {
			writeJSON(w, http.StatusBadRequest, apiResponse{OK: false, Error: err.Error()})
			return
		}
	case "agent_start":
		if err := s.agent.Start(action.Target); err != nil {
			writeJSON(w, http.StatusBadRequest, apiResponse{OK: false, Error: err.Error()})
			return
		}
	case "agent_stop":
		s.agent.Stop()
	case "noop":
	default:
		action.Type = "noop"
	}

	var status workerStatus
	_ = s.worker.call("status", nil, &status)
	agentStatus := s.agent.snapshot()

	writeJSON(w, http.StatusOK, apiResponse{
		OK:      true,
		Status:  &status,
		Action:  &action,
		Agent:   &agentStatus,
		Source:  source,
		Message: "voice command processed",
	})
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

	target := strings.TrimSpace(req.Target)
	if target == "" {
		writeJSON(w, http.StatusBadRequest, apiResponse{OK: false, Error: "target is required"})
		return
	}

	if err := s.agent.Start(target); err != nil {
		writeJSON(w, http.StatusBadRequest, apiResponse{OK: false, Error: err.Error()})
		return
	}

	status := s.agent.snapshot()
	writeJSON(w, http.StatusOK, apiResponse{
		OK:      true,
		Agent:   &status,
		Message: "agent started",
	})
}

func (s *appServer) handleAgentStop(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeJSON(w, http.StatusMethodNotAllowed, apiResponse{OK: false, Error: "method not allowed"})
		return
	}

	s.agent.Stop()
	status := s.agent.snapshot()
	writeJSON(w, http.StatusOK, apiResponse{
		OK:      true,
		Agent:   &status,
		Message: "agent stopped",
	})
}

func (s *appServer) handleAgentStatus(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeJSON(w, http.StatusMethodNotAllowed, apiResponse{OK: false, Error: "method not allowed"})
		return
	}

	status := s.agent.snapshot()
	writeJSON(w, http.StatusOK, apiResponse{OK: true, Agent: &status})
}

func decodeJSON(r *http.Request, target any) error {
	defer r.Body.Close()
	dec := json.NewDecoder(r.Body)
	dec.DisallowUnknownFields()
	if err := dec.Decode(target); err != nil {
		return fmt.Errorf("invalid json body: %w", err)
	}
	return nil
}

func writeJSON(w http.ResponseWriter, code int, payload any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(payload)
}

func normalizeBasePath(raw string) string {
	base := "/" + strings.Trim(strings.TrimSpace(raw), "/")
	if base == "/" {
		return ""
	}
	return base
}

func uniqueSortedStrings(values []string) []string {
	seen := make(map[string]struct{}, len(values))
	out := make([]string, 0, len(values))
	for _, v := range values {
		if _, ok := seen[v]; ok {
			continue
		}
		seen[v] = struct{}{}
		out = append(out, v)
	}
	sort.Strings(out)
	return out
}

func joinPrefix(prefix, suffix string) string {
	if prefix == "" {
		return suffix
	}
	return prefix + suffix
}

func registerRoutes(mux *http.ServeMux, server *appServer, webDir string, prefix string) {
	mux.HandleFunc(joinPrefix(prefix, "/api/status"), server.handleStatus)
	mux.HandleFunc(joinPrefix(prefix, "/api/connect"), server.handleConnect)
	mux.HandleFunc(joinPrefix(prefix, "/api/disconnect"), server.handleDisconnect)
	mux.HandleFunc(joinPrefix(prefix, "/api/command"), server.handleCommand)
	mux.HandleFunc(joinPrefix(prefix, "/api/video/frame"), server.handleVideoFrame)
	mux.HandleFunc(joinPrefix(prefix, "/api/voice/command"), server.handleVoiceCommand)
	mux.HandleFunc(joinPrefix(prefix, "/api/agent/start"), server.handleAgentStart)
	mux.HandleFunc(joinPrefix(prefix, "/api/agent/stop"), server.handleAgentStop)
	mux.HandleFunc(joinPrefix(prefix, "/api/agent/status"), server.handleAgentStatus)

	fileServer := http.FileServer(http.Dir(webDir))
	if prefix == "" {
		// Root deployment: serve UI directly at "/".
		mux.Handle("/", fileServer)
		return
	}

	// Prefixed deployment: serve UI at "<prefix>/web/".
	webMount := joinPrefix(prefix, "/web/")
	mux.Handle(webMount, http.StripPrefix(webMount, fileServer))
	mux.HandleFunc(strings.TrimSuffix(webMount, "/"), func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, webMount, http.StatusTemporaryRedirect)
	})
}

func main() {
	workerScript := filepath.Join("worker", "unitree_worker.py")
	if _, err := os.Stat(workerScript); err != nil {
		log.Fatalf("worker script not found at %s: %v", workerScript, err)
	}

	worker := newWorkerClient(workerScript)
	defer worker.close()

	// Start the worker early so startup issues are visible right away.
	var startupStatus workerStatus
	if err := worker.call("status", nil, &startupStatus); err != nil {
		log.Fatalf("failed to start worker: %v", err)
	}

	gemini := newGeminiClient()
	agent := newAgentController(worker, gemini)
	defer agent.Stop()

	server := &appServer{
		worker: worker,
		gemini: gemini,
		agent:  agent,
	}

	mux := http.NewServeMux()
	webDir := filepath.Join(".", "web")

	// Always expose root routes, and optionally expose a prefixed route set.
	// Example: BASE_PATH=/robot gives /robot/web/ and /robot/api/*.
	basePath := normalizeBasePath(os.Getenv("BASE_PATH"))
	prefixes := uniqueSortedStrings([]string{"", "/robot", basePath})
	for _, prefix := range prefixes {
		registerRoutes(mux, server, webDir, prefix)
	}

	port := strings.TrimSpace(os.Getenv("PORT"))
	if port == "" {
		port = defaultPort
	}

	addr := ":" + port
	log.Printf("Server listening on http://localhost%s", addr)
	log.Printf("Gemini model: %s | enabled: %v", gemini.model, gemini.enabled())
	for _, prefix := range prefixes {
		if prefix == "" {
			log.Printf("UI: http://localhost%s/ | API: http://localhost%s/api/*", addr, addr)
		} else {
			log.Printf("UI: http://localhost%s%s/web/ | API: http://localhost%s%s/api/*", addr, prefix, addr, prefix)
		}
	}
	if err := http.ListenAndServe(addr, mux); err != nil {
		log.Fatal(err)
	}
}
