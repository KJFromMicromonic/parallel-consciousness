package protocol

import "testing"

func TestReadyIntentValue(t *testing.T) {
	if IntentReady != "ready" {
		t.Fatalf("IntentReady = %q, want %q", IntentReady, "ready")
	}
}

func TestReadyIntentIsTerminal(t *testing.T) {
	if IntentReady.WantsResponse() {
		t.Fatal("IntentReady must be terminal (WantsResponse should be false)")
	}
}
