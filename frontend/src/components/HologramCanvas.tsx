// src/components/HologramCanvas.tsx
import React, { useEffect, useRef } from "react";
import { NodeType, LinkType } from "../hooks/useHologramSocket";

type Props = {
  width?: number;
  height?: number;
  nodes: NodeType[];
  links: LinkType[];
  backgroundColor?: string;
};

type RenderNode = NodeType & {
  x: number;
  y: number;
  // visual transition state
  visibleColor?: string;
  spawnProgress?: number; // 0..1 for spawn
  deactivateProgress?: number; // 0..1 for deactivate
  perDotOffsets?: number[]; // jitter per-dot
};

type RenderLink = LinkType & {
  // computed geometry
  srcX?: number;
  srcY?: number;
  tgtX?: number;
  tgtY?: number;
  trailOffsets?: number[];
};

const defaultBg = "#0b0c0f";

function hexToRGBA(hex: string, a = 1) {
  // accept #RRGGBB
  const h = hex.replace("#", "");
  const r = parseInt(h.substring(0, 2), 16);
  const g = parseInt(h.substring(2, 4), 16);
  const b = parseInt(h.substring(4, 6), 16);
  return `rgba(${r},${g},${b},${a})`;
}

export const HologramCanvas: React.FC<Props> = ({ width = 1200, height = 700, nodes, links, backgroundColor = defaultBg }) => {
  const canvasRef = useRef<HTMLCanvasElement | null>(null);
  const renderNodesRef = useRef<Record<string, RenderNode>>({});
  const renderLinksRef = useRef<Record<string, RenderLink>>({});
  const lastUpdateRef = useRef<number>(performance.now());
  const animationRef = useRef<number | null>(null);

  // layout constants
  const padding = 80;

  // helper: deterministic random by id
  function seededRand(seed: string) {
    let h = 2166136261 >>> 0;
    for (let i = 0; i < seed.length; i++) {
      h = Math.imul(h ^ seed.charCodeAt(i), 16777619);
    }
    return function () {
      h += h << 13; h ^= h >>> 7; h += h << 3; h ^= h >>> 17; h += h << 5;
      return (h >>> 0) / 4294967295;
    };
  }

  // initialize/merge nodes into renderNodesRef when nodes change
  useEffect(() => {
    // create / update renderNodes store
    const store = renderNodesRef.current;
    // compute available layout positions deterministically for now
    const cols = Math.max(3, Math.round(Math.sqrt(nodes.length)));
    const rows = Math.ceil(nodes.length / cols);
    nodes.forEach((n, idx) => {
      if (!store[n.id]) {
        const rng = seededRand(n.id);
        const col = idx % cols;
        const row = Math.floor(idx / cols);
        // jitter positions
        const x = padding + (col + rng() * 0.8) * ((width - padding * 2) / Math.max(1, cols - 1));
        const y = padding + (row + rng() * 0.8) * ((height - padding * 2) / Math.max(1, rows - 1));
        store[n.id] = {
          ...n,
          x,
          y,
          visibleColor: "#ffffff",
          spawnProgress: 0,
          deactivateProgress: 0,
          perDotOffsets: Array(Math.max(4, Math.floor((n.size || 6) * 2))).fill(0).map(() => rng())
        };
      } else {
        // update dynamic fields
        store[n.id] = { ...store[n.id], ...n };
      }
    });
    // remove nodes that no longer exist
    const existingIds = new Set(nodes.map((x) => x.id));
    Object.keys(store).forEach((k) => { if (!existingIds.has(k)) delete store[k]; });

    // links
    const lstore = renderLinksRef.current;
    links.forEach((l) => {
      if (!lstore[l.id]) {
        lstore[l.id] = { ...l, trailOffsets: Array(Math.max(2, l.dot_density || 3)).fill(0).map(() => Math.random()) };
      } else {
        lstore[l.id] = { ...lstore[l.id], ...l };
      }
    });
    Object.keys(lstore).forEach((k) => { if (!links.find((x) => x.id === k)) delete lstore[k]; });

  }, [nodes, links, width, height]);

  useEffect(() => {
    const ctx = canvasRef.current?.getContext("2d");
    if (!ctx) return;

    let last = performance.now();
    function step(now: number) {
      const dt = Math.min(60, now - last);
      last = now;
      drawFrame(ctx, now, dt);
      animationRef.current = requestAnimationFrame(step);
    }
    animationRef.current = requestAnimationFrame(step);
    return () => {
      if (animationRef.current) cancelAnimationFrame(animationRef.current);
    };
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [canvasRef.current]);

  function drawFrame(ctx: CanvasRenderingContext2D, now: number, dt: number) {
    // clear
    const dpr = window.devicePixelRatio || 1;
    const canvas = ctx.canvas;
    if (canvas.width !== width * dpr || canvas.height !== height * dpr) {
      canvas.width = width * dpr;
      canvas.height = height * dpr;
      canvas.style.width = `${width}px`;
      canvas.style.height = `${height}px`;
      ctx.setTransform(dpr, 0, 0, dpr, 0, 0);
    }
    ctx.clearRect(0, 0, width, height);
    // background
    ctx.fillStyle = backgroundColor;
    ctx.fillRect(0, 0, width, height);

    // update and draw links first (so nodes are drawn on top)
    const rlinks = Object.values(renderLinksRef.current) as RenderLink[];
    const rnodes = renderNodesRef.current;

    // recompute geometry for links
    for (const l of rlinks) {
      const s = rnodes[l.source];
      const t = rnodes[l.target];
      if (!s || !t) continue;
      l.srcX = s.x;
      l.srcY = s.y;
      l.tgtX = t.x;
      l.tgtY = t.y;
    }

    // draw links (gradient stroke + traveling dots)
    for (const l of rlinks) {
      if (!l.srcX || !l.tgtX) continue;
      const sX = l.srcX!;
      const sY = l.srcY!;
      const tX = l.tgtX!;
      const tY = l.tgtY!;
      // gradient
      const grad = ctx.createLinearGradient(sX, sY, tX, tY);
      const c0 = l.gradient && l.gradient[0] ? l.gradient[0] : "#888";
      const c1 = l.gradient && l.gradient[1] ? l.gradient[1] : "#333";
      grad.addColorStop(0, hexToRGBA(c0, l.state === "active" ? 0.95 : 0.25));
      grad.addColorStop(1, hexToRGBA(c1, l.state === "active" ? 0.95 : 0.25));
      ctx.strokeStyle = grad;
      ctx.lineWidth = Math.max(0.5, (l.intensity || 0.05) * 8);
      ctx.lineCap = "round";
      ctx.beginPath();
      ctx.moveTo(sX, sY);
      ctx.lineTo(tX, tY);
      ctx.stroke();

      // moving dots along link
      const dotCount = Math.max(1, l.dot_density || 2);
      const length = Math.hypot(tX - sX, tY - sY);
      for (let i = 0; i < dotCount; i++) {
        const speed = 0.0008 * (100 + (l.intensity || 0.05) * 800); // robust speed mapping
        // compute position progress using time + offset
        const base = ((now * (0.0003 + 0.0006 * i)) + (l.trailOffsets ? (l.trailOffsets[i % l.trailOffsets.length] * 1000) : 0));
        const progress = (base * speed) % 1;
        const px = sX + (tX - sX) * progress;
        const py = sY + (tY - sY) * progress;
        // dot color based on interpolated gradient
        // simple mix: draw with alpha
        ctx.beginPath();
        ctx.fillStyle = hexToRGBA(c0, 0.9 - (i * 0.08));
        ctx.arc(px, py, Math.max(0.9, (l.intensity || 0.05) * 6), 0, Math.PI * 2);
        ctx.fill();
      }
    }

    // draw nodes (each as many tiny dots)
    const nodesArr = Object.values(renderNodesRef.current) as RenderNode[];

    for (const n of nodesArr) {
      // update spawn/deactivate timing based on n.state
      if (n.state === "spawning") {
        n.spawnProgress = Math.min(1, (n.spawnProgress || 0) + (dt / 900));
        // visible color fades from white to target color
        const t = n.spawnProgress;
        // simple lerp from white to color using mix of rgba; frontend interprets color strings
        // we'll use alpha layering: draw a soft halo then dots tinted
      } else {
        n.spawnProgress = 1;
      }
      if (n.state === "deactivated") {
        n.deactivateProgress = Math.min(1, (n.deactivateProgress || 0) + (dt / 1200));
      } else {
        n.deactivateProgress = 0;
      }

      // pick base color: if error state color already set to ERROR_COLOR, use it, else use component color
      const baseColor = n.color || "#ffffff";
      const isError = n.state === "error";
      // actual intensity overlay (breathing + flicker)
      const baseIntensity = n.intensity ?? 0.2;
      let flicker = 0.95 + 0.06 * Math.sin((now * 0.04) + (n.x + n.y) * 0.001) + (Math.random() * 0.06 - 0.03);
      flicker = Math.max(0.6, Math.min(1.4, flicker));
      let breath = 1.0 + 0.15 * Math.sin(now * 0.002 + (n.x * 0.01));
      if (n.state === "active") {
        // active increases intensity
        breath *= 1.2;
      } else if (n.state === "idle") {
        breath *= 0.9;
      } else if (n.state === "spawning") {
        breath *= 1.0 + (1 - (n.spawnProgress || 0));
      } else if (n.state === "deactivated") {
        breath *= 0.3 * (1 - (n.deactivateProgress || 0));
      } else if (isError) {
        breath *= 1.4 * Math.abs(Math.sin(now * 0.01)); // staccato red pulse
      }

      const intensity = Math.max(0.05, baseIntensity * breath * flicker);

      // halo
      const haloR = Math.max(6, (n.size || 6) * 1.6);
      ctx.beginPath();
      ctx.fillStyle = hexToRGBA(baseColor, Math.max(0.02, 0.06 * intensity));
      ctx.arc(n.x, n.y, haloR, 0, Math.PI * 2);
      ctx.fill();

      // draw many tiny dots around the node center
      const dotCount = Math.max(6, Math.floor((n.size || 6) * (1 + intensity * 6)));
      for (let i = 0; i < dotCount; i++) {
        const angle = (i / dotCount) * Math.PI * 2 + (now * 0.0002) * (i % 3);
        const radius = (n.size || 6) * 0.3 + ((i % 3) * 0.9) + (Math.sin(now * 0.001 + i) * 0.8);
        const dx = Math.cos(angle) * radius * (0.5 + 0.5 * Math.random());
        const dy = Math.sin(angle) * radius * (0.5 + 0.5 * Math.random());
        const px = n.x + dx;
        const py = n.y + dy;

        // color mix between white (spawn) and baseColor based on spawnProgress/deactivate
        let alpha = 0.4 * intensity * (0.9 + Math.random() * 0.3);
        if (n.spawnProgress !== undefined) {
          alpha *= 0.4 + 0.6 * (n.spawnProgress || 0);
        }
        if (n.deactivateProgress !== undefined && n.deactivateProgress > 0) {
          alpha *= 1 - (n.deactivateProgress || 0);
        }
        // if error state, color is red and brighter
        const fill = isError ? hexToRGBA("#ff0000", Math.min(1.0, alpha * 1.4)) : hexToRGBA(baseColor, alpha);

        ctx.beginPath();
        ctx.fillStyle = fill;
        const r = Math.max(0.7, Math.random() * 2.4 * (intensity));
        ctx.arc(px, py, r, 0, Math.PI * 2);
        ctx.fill();
      }

      // small label text (optional)
      ctx.fillStyle = "rgba(255,255,255,0.8)";
      ctx.font = "11px Inter, Arial";
      ctx.textAlign = "center";
      ctx.fillText(n.label || n.type, n.x, n.y + (n.size || 6) + 12);
    }
  }

  return (
    <canvas
      ref={canvasRef}
      width={width}
      height={height}
      style={{
        width: `${width}px`,
        height: `${height}px`,
        display: "block",
        borderRadius: 12,
        background: backgroundColor,
      }}
    />
  );
};
