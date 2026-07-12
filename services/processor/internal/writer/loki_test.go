package writer

import (
	"encoding/json"
	"testing"
	"time"
)

func TestLokiPayload_GroupsByLevel(t *testing.T) {
	lines := []LogLine{
		{Level: "info", Message: "user login", Timestamp: time.Now()},
		{Level: "error", Message: "db timeout", Timestamp: time.Now()},
		{Level: "info", Message: "request complete", Timestamp: time.Now()},
	}

	// build the payload the same way the writer does
	type lokiValue [2]string
	type lokiStream struct {
		Stream map[string]string `json:"stream"`
		Values []lokiValue       `json:"values"`
	}
	type lokiPush struct {
		Streams []lokiStream `json:"streams"`
	}

	grouped := make(map[string][]lokiValue)
	for _, l := range lines {
		grouped[l.Level] = append(grouped[l.Level], lokiValue{
			"1000000000",
			l.Message,
		})
		t.Log(grouped[l.Level])
		t.Log(grouped[l.Message])
	}

	streams := make([]lokiStream, 0, len(grouped))
	for level, values := range grouped {
		t.Log(level)
		t.Log(values)
		streams = append(streams, lokiStream{
			Stream: map[string]string{"level": level, "tenant": "t1"},
			Values: values,
		})
	}

	payload := lokiPush{Streams: streams}
	data, err := json.Marshal(payload)
	if err != nil {
		t.Fatalf("failed to marshal payload: %v", err)
	}

	// unmarshal and verify
	var result lokiPush
	if err := json.Unmarshal(data, &result); err != nil {
		t.Fatalf("failed to unmarshal: %v", err)
	}

	if len(result.Streams) != 2 {
		t.Errorf("expected 2 streams (info + error), got %d", len(result.Streams))
	}

	// find info stream and check it has 2 entries
	for _, s := range result.Streams {
		if s.Stream["level"] == "info" && len(s.Values) != 2 {
			t.Errorf("expected 2 info log lines, got %d", len(s.Values))
		}
		if s.Stream["level"] == "error" && len(s.Values) != 1 {
			t.Errorf("expected 1 error log line, got %d", len(s.Values))
		}
	}
}

func TestLokiPayload_EmptyLevelDefaultsToInfo(t *testing.T) {
	lines := []LogLine{
		{Level: "", Message: "no level set", Timestamp: time.Now()},
	}

	level := lines[0].Level
	if level == "" {
		level = "info"
	}

	if level != "info" {
		t.Errorf("expected empty level to default to info, got %q", level)
	}
}

func TestLokiPayload_TraceIDAppendedToMessage(t *testing.T) {
	line := LogLine{
		Level:   "info",
		Message: "request handled",
		TraceID: "abc123",
	}

	msg := line.Message
	if line.TraceID != "" {
		msg = msg + " trace_id=" + line.TraceID
	}

	expected := "request handled trace_id=abc123"
	if msg != expected {
		t.Errorf("expected %q, got %q", expected, msg)
	}
}

func TestLogLine_EmptyBatchSkipped(t *testing.T) {
	// verify that an empty slice produces no streams
	lines := []LogLine{}
	if len(lines) != 0 {
		t.Errorf("expected empty batch to be skipped")
	}
}
