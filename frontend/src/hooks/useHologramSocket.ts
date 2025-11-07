// src/hooks/useHologramSocket.ts
import { useEffect, useRef, useState } from "react";

export type NodeType = {
  id: string;
  label: string;
  type: string;
  component: string;
  color: string;
  size: number;
  intensity: number;
  state: string;
  spawn_time: number;
  metadata: any;
};

export type LinkType = {
  id: string;
  source: string;
  target: string;
  gradient: string[];
  intensity: number;
  dot_density: number;
  state: string;
};

export function useHologramSocket(wsUrl: string) {
  const wsRef = useRef<WebSocket | null>(null);
  const [nodes, setNodes] = useState<NodeType[]>([]);
  const [links, setLinks] = useState<LinkType[]>([]);
  const reconnectTimer = useRef<number | null>(null);

  useEffect(() => {
    function connect() {
      wsRef.current = new WebSocket(wsUrl);
      wsRef.current.onopen = () => {
        console.log("[hologram] ws open");
      };
      wsRef.current.onmessage = (ev) => {
        try {
          const msg = JSON.parse(ev.data);
          if (msg.type === "snapshot") {
            const data = msg.data;
            if (data.nodes) setNodes(data.nodes);
            if (data.links) setLinks(data.links);
          }
        } catch (e) {
          console.warn("[hologram] ws parse", e);
        }
      };
      wsRef.current.onclose = () => {
        console.log("[hologram] ws closed, retrying in 1s");
        if (reconnectTimer.current) window.clearTimeout(reconnectTimer.current);
        reconnectTimer.current = window.setTimeout(connect, 1000);
      };
      wsRef.current.onerror = (ev) => {
        console.error("[hologram] ws error", ev);
        wsRef.current?.close();
      };
    }

    connect();

    return () => {
      if (reconnectTimer.current) window.clearTimeout(reconnectTimer.current);
      wsRef.current?.close();
    };
  }, [wsUrl]);

  return { nodes, links };
}
