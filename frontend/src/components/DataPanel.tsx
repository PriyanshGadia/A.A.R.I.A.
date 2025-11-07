import { ReactNode, useState, useEffect } from 'react';
import { Rnd, DraggableData, RndResizeCallback } from 'react-rnd';

interface DataPanelProps {
  title?: string; // Title is optional
  children: ReactNode;
  default: { x: number; y: number; width: number; height: number; };
  id?: string;
  noPadding?: boolean;
  frameless?: boolean; // Prop for frameless mode
}

export function DataPanel({ title, children, default: defaultProps, id, noPadding, frameless }: DataPanelProps) {
  const [position, setPosition] = useState({ x: defaultProps.x, y: defaultProps.y });
  const [size, setSize] = useState({ width: defaultProps.width, height: defaultProps.height });

  useEffect(() => {
    // Guard for environments where window may be undefined (SSR)
    const winWidth = typeof window !== "undefined" ? window.innerWidth : defaultProps.width;
    const winHeight = typeof window !== "undefined" ? window.innerHeight : defaultProps.height;

    setPosition({
        x: Math.min(Math.max(0, defaultProps.x), winWidth - defaultProps.width),
        y: Math.min(Math.max(0, defaultProps.y), winHeight - defaultProps.height)
    });
    setSize({
        width: Math.min(winWidth, defaultProps.width),
        height: Math.min(winHeight, defaultProps.height)
    });
  }, [defaultProps.x, defaultProps.y, defaultProps.width, defaultProps.height]);

  const handleDragStop = (_e: any, d: DraggableData) => {
    setPosition({ x: d.x, y: d.y });
  };

  const handleResizeStop: RndResizeCallback = (_e, _direction, ref, _delta, newPosition) => {
    // parseFloat is safer and handles fractional px
    setSize({
      width: parseFloat(ref.style.width || `${defaultProps.width}`),
      height: parseFloat(ref.style.height || `${defaultProps.height}`),
    });
    setPosition({ x: newPosition.x, y: newPosition.y });
  };

  const panelClasses = frameless
    ? "absolute z-20 overflow-hidden"
    : "absolute backdrop-blur-md bg-panel-bg holographic-panel z-20 overflow-hidden";

  const panelStyles = frameless
    ? {}
    : {
        '--panel-bg-color': 'rgba(15, 15, 25, 0.85)',
        '--panel-border-color': 'rgba(0, 255, 255, 0.4)',
        '--panel-glow-color': 'rgba(0, 255, 255, 0.4)',
      } as React.CSSProperties;

  const handleStyles = frameless
    ? {
        bottomRight: { width: '10px', height: '10px', background: 'transparent', cursor: 'nwse-resize' },
        bottomLeft:  { width: '10px', height: '10px', background: 'transparent', cursor: 'nesw-resize' },
        topRight:    { width: '10px', height: '10px', background: 'transparent', cursor: 'nesw-resize' },
        topLeft:     { width: '10px', height: '10px', background: 'transparent', cursor: 'nwse-resize' },
        right:       { width: '5px', background: 'transparent', cursor: 'ew-resize' },
        left:        { width: '5px', background: 'transparent', cursor: 'ew-resize' },
        top:         { height: '5px', background: 'transparent', cursor: 'ns-resize' },
        bottom:      { height: '5px', background: 'transparent', cursor: 'ns-resize' },
      }
    : {
        bottomRight: { width: '10px', height: '10px', background: 'linear-gradient(45deg, rgba(0,255,255,0.8), rgba(255,0,255,0.5))', borderRadius: '2px', opacity: 0.8 },
        bottomLeft:  { width: '10px', height: '10px', background: 'linear-gradient(135deg, rgba(0,255,255,0.8), rgba(255,0,255,0.5))', borderRadius: '2px', opacity: 0.8 },
        topRight:    { width: '10px', height: '10px', background: 'linear-gradient(135deg, rgba(0,255,255,0.8), rgba(255,0,255,0.5))', borderRadius: '2px', opacity: 0.8 },
        topLeft:     { width: '10px', height: '10px', background: 'linear-gradient(45deg, rgba(0,255,255,0.8), rgba(255,0,255,0.5))', borderRadius: '2px', opacity: 0.8 },
        right:       { width: '5px', background: 'rgba(0,255,255,0.3)', opacity: 0.6 },
        left:        { width: '5px', background: 'rgba(0,255,255,0.3)', opacity: 0.6 },
        top:         { height: '5px', background: 'rgba(0,255,255,0.3)', opacity: 0.6 },
        bottom:      { height: '5px', background: 'rgba(0,255,255,0.3)', opacity: 0.6 },
      };

  return (
    <Rnd
      size={{ width: size.width, height: size.height }}
      position={{ x: position.x, y: position.y }}
      onDragStop={handleDragStop}
      onResizeStop={handleResizeStop}
      minWidth={200}
      minHeight={200}
      bounds="parent"
      dragHandleClassName={frameless ? undefined : "panel-handle"}
      className={panelClasses}
      style={panelStyles}
      resizeHandleStyles={handleStyles}
    >
      <div className={`flex flex-col h-full w-full ${noPadding ? 'p-0' : 'p-4'}`}>
        {!frameless && title && (
          <div className={`panel-handle flex items-center justify-between cursor-grab flex-shrink-0 ${noPadding ? 'p-4' : 'mb-2'}`}>
            <h2 className="text-glow-secondary text-lg font-bold uppercase tracking-wide">{title}</h2>
            <span className="text-text-secondary text-xs opacity-50">:: DRAG ::</span>
          </div>
        )}
        <div className={`flex-grow text-text-primary text-sm overflow-auto ${noPadding ? 'h-full' : 'custom-scrollbar'}`}>
          {children}
        </div>
      </div>
    </Rnd>
  );
}
