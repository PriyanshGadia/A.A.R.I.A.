// src/components/HologramPanel.tsx
import React, { useMemo, useState } from "react";
import { DataPanel } from "./DataPanel";
import { SystemState } from "../hooks/useAariaSocket";
import { Hologram } from "./Hologram"; // your improved Hologram.tsx

type Props = {
  systemState: SystemState;
};

export const HologramPanel: React.FC<Props> = ({ systemState }) => {
  const [paused, setPaused] = useState(false);
  const [zoom, setZoom] = useState(1.0);

  const layoutWidth = Math.min(window.innerWidth - 120, 1280);
  const layoutHeight = Math.min(window.innerHeight - 160, 780);

  // small memo: transform systemState to the same shape Hologram expects
  const memoState = useMemo(() => systemState, [systemState]);

  return (
    <DataPanel
      id="hologram-panel"
      title="Hologram Plexus"
      default={{ x: window.innerWidth / 2 - layoutWidth / 2, y: 60, width: layoutWidth, height: layoutHeight }}
    >
      <div className="flex flex-col h-full">
        <div className="flex items-center justify-between px-2 py-1 border-b border-panel-border/20">
          <div className="flex items-center gap-2">
            <span className="text-sm text-text-primary">Plexus</span>
            <span className="text-xs text-text-secondary">Real-time node / link view</span>
          </div>
          <div className="flex items-center gap-2">
            <label className="text-xs text-text-secondary">Zoom</label>
            <input
              type="range"
              min={0.6}
              max={1.6}
              step={0.05}
              value={zoom}
              onChange={(e) => setZoom(parseFloat(e.target.value))}
            />
            <button
              onClick={() => { setPaused((s) => !s); }}
              className="bg-glow-secondary/30 px-2 py-1 rounded text-xs text-white"
            >
              {paused ? "Resume" : "Pause"}
            </button>
          </div>
        </div>

        <div className="flex-grow p-2">
          {/* Hologram component (Three.js) */}
          <div style={{ width: "100%", height: "100%" }}>
            {!paused ? (
              <Hologram systemState={memoState} />
            ) : (
              <div className="w-full h-full flex items-center justify-center text-text-secondary">
                Visualization paused
              </div>
            )}
          </div>
        </div>
      </div>
    </DataPanel>
  );
};
