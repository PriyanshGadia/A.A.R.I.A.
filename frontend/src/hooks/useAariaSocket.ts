// src/hooks/useAariaSocket.ts
import { useEffect, useRef, useState } from "react";
import { io, Socket } from "socket.io-client";

export type HologramNode = {
  id: string;
  label: string;
  type: string;
  component: string;
  color: string;
  size: number;
  intensity: number;
  state: string;
  spawn_time: number;
  metadata?: any;
};

export type HologramLink = {
  id: string;
  source: string;
  target: string;
  gradient: string[];
  intensity: number;
  dot_density: number;
  state: string;
};

export type SystemState = {
  nodes: HologramNode[];
  links: HologramLink[];
};

export type AssistantResponse = {
  content: string;
  metadata?: any;
};

export function useAariaSocket(serverUrl = "http://127.0.0.1:1420") {
  const socketRef = useRef<Socket | null>(null);
  const [connected, setConnected] = useState(false);
  const [lastResponse, setLastResponse] = useState<AssistantResponse | null>(null);
  const [initialLayout, setInitialLayout] = useState<string[]>([]);
  const [error, setError] = useState<string | null>(null);
  const [systemState, setSystemState] = useState<SystemState>({ nodes: [], links: [] });

  useEffect(() => {
    // connect with auto-reconnect enabled
    const socket = io(serverUrl, {
      reconnectionAttempts: Infinity,
      reconnectionDelay: 1000,
      transports: ["websocket", "polling"],
    });

    socketRef.current = socket;

    socket.on("connect", () => {
      setConnected(true);
      setError(null);
      console.debug("[AARIA socket.io] connected", socket.id);
    });

    socket.on("disconnect", (reason: any) => {
      setConnected(false);
      console.warn("[AARIA socket.io] disconnected:", reason);
    });

    socket.on("connect_error", (err: any) => {
      setError(String(err));
      console.error("[AARIA socket.io] connect_error", err);
    });

    // App-level messages
    socket.on("assistant_response", (payload: any) => {
      try {
        const msg = payload as AssistantResponse;
        setLastResponse(msg);
      } catch (e) {
        console.warn("assistant_response parse error", e);
      }
    });

    // UI init: includes layout array (comms, security, insights, hologram)
    socket.on("ui_init", (payload: any) => {
      try {
        const layout = payload?.layout;
        if (Array.isArray(layout)) setInitialLayout(layout);
      } catch (e) {
        console.warn("ui_init parse error", e);
      }
    });

    // Hologram state updates
    socket.on("system_state_update", (payload: any) => {
      try {
        const data = payload as SystemState;
        setSystemState({ nodes: data.nodes || [], links: data.links || [] });
      } catch (e) {
        console.warn("system_state_update parse error", e);
      }
    });

    // System errors
    socket.on("system_error", (payload: any) => {
      console.error("[AARIA system_error]", payload);
      setError(payload?.error ? String(payload.error) : "Unknown system error");
    });

    return () => {
      socket.disconnect();
      socketRef.current = null;
    };
  }, [serverUrl]);

  // Helper: send a user message to backend via Socket.IO event
  async function sendMessage(text: string, opts: any = {}) {
    const socket = socketRef.current;
    if (!socket || !socket.connected) {
      setError("Not connected");
      return;
    }
    // The backend expects event 'user_message' with { content, user_id, user_name, ... }
    socket.emit("user_message", {
      content: text,
      user_id: opts.user_id || "owner_primary",
      user_name: opts.user_name || "Owner",
      metadata: opts,
    });
  }

  // Helper: TOTP verification
  async function sendVerification(code: string) {
    const socket = socketRef.current;
    if (!socket || !socket.connected) {
      setError("Not connected");
      return;
    }
    // emit verify_totp with code
    socket.emit("verify_totp", code);
  }

  // Expose a manual emitter in case you want to call commands directly
  function emitRaw(event: string, payload?: any) {
    const socket = socketRef.current;
    if (!socket) return;
    socket.emit(event, payload);
  }

  return {
    isConnected: connected,
    lastResponse,
    error,
    initialLayout,
    sendMessage,
    sendVerification,
    systemState,
    emitRaw,
  };
}
