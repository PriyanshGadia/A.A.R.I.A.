import { useState, useEffect, FormEvent } from 'react';
import { WebviewWindow } from '@tauri-apps/api/webviewWindow'; 
import { DataPanel } from './components/DataPanel';
import { HologramPanel } from './components/HologramPanel'; // NEW
import { useAariaSocket, AssistantResponse, SystemState } from './hooks/useAariaSocket'; 
import { TOTPScreen } from './components/TOTPScreen';
import { PlayIcon, StopIcon } from "@heroicons/react/24/solid";
import { useRef } from "react";

const currentWindow = WebviewWindow.getCurrent(); 

function App() {
  const [isVerified, setIsVerified] = useState(false); 
  const [showPanels, setShowPanels] = useState(false); 
  
  const { 
    isConnected, 
    lastResponse, 
    error, 
    initialLayout, 
    sendMessage, 
    sendVerification,
    systemState // This is now { nodes, links }
  } = useAariaSocket();

  const [currentMessage, setCurrentMessage] = useState(""); 
  const [chatHistory, setChatHistory] = useState<AssistantResponse[]>([]); 
  const [isInteractive, setIsInteractive] = useState(true); 

  useEffect(() => {
    if (lastResponse) {
      setChatHistory(prev => [...prev, lastResponse]);
    }
  }, [lastResponse]);

  useEffect(() => {
    if (initialLayout.length > 0) {
      setIsVerified(true); 
      const timer = setTimeout(() => setShowPanels(true), 500);
      return () => clearTimeout(timer); 
    }
  }, [initialLayout]); 

  // Effect to manage the Tauri window's interactive (click-through) state
  const permissionLoggedRef = useRef(false);

useEffect(() => {
  const toggleWindowInteraction = async () => {
    if (!currentWindow) return;

    // If the API is unavailable (older webview/t_wrong allowlist), guard it
    if (typeof (currentWindow as any).setIgnoreCursorEvents !== "function") {
      if (!permissionLoggedRef.current) {
        console.warn("AARIA Shell: setIgnoreCursorEvents API not available in this runtime. Falling back to UI-only mode.");
        permissionLoggedRef.current = true;
      }
      return;
    }

    try {
      // call the Tauri API; if it's not permitted this will throw
      await (currentWindow as any).setIgnoreCursorEvents(!isInteractive);
      // Optional: log only in dev
      // console.debug(`AARIA Shell: Interaction mode set to ${isInteractive ? 'interactive' : 'click-through'}`);
    } catch (err: any) {
      // Permission denied or other runtime error — log once and continue
      if (!permissionLoggedRef.current) {
        console.warn("AARIA Shell: Failed to set window interaction (permission denied or unsupported). Continuing without ignoring cursor events.", err?.message ?? err);
        permissionLoggedRef.current = true;
      }
      // No rethrow — we gracefully continue without the API
    }
  };

  toggleWindowInteraction();
}, [isInteractive]);

  const handleChatSubmit = (e: FormEvent) => {
    e.preventDefault(); 
    if (!currentMessage.trim()) return; 

    setChatHistory(prev => [...prev, { 
      content: currentMessage, 
      metadata: { sender: "user", timestamp: new Date().toISOString() } 
    }]);

    sendMessage(currentMessage, { user_name: "Owner" }); 
    setCurrentMessage(""); 
  };

  const handleVerification = (code: string) => {
    sendVerification(code); 
  };

  const showPanel = (name: string) => initialLayout.includes(name);

  const handleDiagnostics = () => {
    sendMessage("DIAGNOSTICS_REQUEST", { user_name: "Owner" });
  };

  const handleSuspendCore = () => {
    sendMessage("SUSPEND_CORE", { user_name: "Owner" });
  };

  if (!isVerified) {
    return <TOTPScreen onVerify={handleVerification} error={error} isConnected={isConnected} />
  }

  return (
    <div className="relative w-screen h-screen overflow-hidden bg-transparent">
      {/* MODIFIED: bg-base-bg removed, now fully transparent */}

      <div className={`relative z-10 w-full h-full transition-opacity duration-1000 ${showPanels ? 'opacity-100' : 'opacity-0'}`}>
        
        {showPanel('comms') && (
          <DataPanel 
            id="comms-panel" 
            title="Communications" 
            default={{ x: 50, y: 50, width: 450, height: 400 }} 
          >
            <div className="flex flex-col h-full">
              <div className="flex flex-col-reverse flex-grow space-y-2 space-y-reverse overflow-y-auto custom-scrollbar pr-2">
                {chatHistory.slice().reverse().map((msg, index) => (
                  <div key={index} className={`p-2 rounded max-w-[80%] ${
                    msg.metadata?.sender === "user" 
                      ? "bg-blue-700/30 self-end text-right" 
                      : "bg-glow-primary/30 self-start text-left" 
                  }`}>
                    <p className="text-text-primary">{msg.content}</p>
                    {msg.metadata?.timestamp && (
                        <p className="text-xs text-text-secondary opacity-70 mt-1">
                            {new Date(msg.metadata.timestamp).toLocaleTimeString()}
                        </p>
                    )}
                  </div>
                ))}
              </div>
              <form onSubmit={handleChatSubmit} className="flex-shrink-0 flex gap-2 pt-2">
                <input
                  type="text"
                  value={currentMessage}
                  onChange={(e) => setCurrentMessage(e.target.value)}
                  className="flex-grow bg-base-bg border border-panel-border rounded px-2 py-1 focus:outline-none focus:border-glow-secondary"
                  placeholder={isConnected ? "Message A.A.R.I.A..." : "Connecting..."}
                  disabled={!isConnected} 
                />
                <button 
                  type="submit" 
                  className="bg-glow-secondary text-white px-3 py-1 rounded disabled:opacity-50" 
                  disabled={!isConnected} 
                >
                  Send
                </button>
              </form>
            </div>
          </DataPanel>
        )}
        
        {/* NEW: Add the frameless Hologram Panel */}
        {showPanel('hologram') && (
           <HologramPanel systemState={systemState} />
        )}

        {showPanel('security') && (
          <DataPanel 
            id="security-panel" 
            title="System Security" 
            default={{ x: window.innerWidth - 400, y: 50, width: 350, height: 200 }} 
          >
            <p>Status: <span className="text-green-400">VERIFIED</span></p> 
            <p className="text-sm text-text-secondary mt-2">
              {error ? `Error: ${error}` : "All systems nominal. No threats detected."} 
            </p>
          </DataPanel>
        )}

        {/* MODIFIED: This panel is now "Hologram Stats" and reads the new state */}
        {showPanel('insights') && ( 
          <DataPanel
            id="core-status-panel" 
            title="Hologram Stats"
            default={{ x: window.innerWidth / 2 - 175, y: window.innerHeight - 250, width: 350, height: 200 }} 
          >
            <div className="flex flex-col gap-2 h-full">
              <div className="flex-grow overflow-y-auto custom-scrollbar pr-2">
                
                <div className="flex items-center justify-between p-1 border-b border-panel-border/30">
                  <span className="text-sm text-text-primary">Cognitive Nodes:</span>
                  <span className="text-lg font-bold text-glow-secondary">
                    {systemState.nodes?.length || 0}
                  </span>
                </div>
                <div className="flex items-center justify-between p-1 border-b border-panel-border/30">
                  <span className="text-sm text-text-primary">Data Links:</span>
                  <span className="text-lg font-bold text-glow-secondary">
                    {systemState.links?.length || 0}
                  </span>
                </div>
                
                <p className="text-text-primary text-sm mb-1 mt-4">Node Details:</p>
                {/* FIX: Check if systemState.nodes exists before mapping */}
                {systemState.nodes?.map(node => (
                  <div key={node.id} className="flex items-center justify-between p-1 border-b border-panel-border/30 last:border-b-0">
                    <span className="text-sm text-text-primary">{node.label}</span>
                    <div className="flex items-center gap-2">
                      <span className={`text-xs font-bold`} style={{ color: node.color }}>
                        {node.type.toUpperCase()}
                      </span>
                      <div className="w-16 bg-base-bg rounded-full h-2">
                        <div 
                          className={`h-full rounded-full`} 
                          style={{ width: `${node.intensity * 100}%`, backgroundColor: node.color }}
                        ></div>
                      </div>
                      <span className="text-xs text-text-secondary w-8 text-right">
                        {(node.intensity * 100).toFixed(0)}%
                      </span>
                    </div>
                  </div>
                ))}

              </div>
              <div className="flex gap-2 mt-auto flex-shrink-0"> 
                <button 
                  onClick={handleDiagnostics}
                  className="flex-grow bg-blue-600/50 text-white text-xs px-2 py-1 rounded-sm hover:bg-blue-500/70 transition-colors"
                >
                  <PlayIcon className="inline-block w-3 h-3 mr-1" /> Diagnostics
                </button>
                <button 
                  onClick={handleSuspendCore}
                  className="flex-grow bg-red-600/50 text-white text-xs px-2 py-1 rounded-sm hover:bg-red-500/70 transition-colors"
                >
                  <StopIcon className="inline-block w-3 h-3 mr-1" /> Suspend Autonomy
                </button>
              </div>
            </div>
          </DataPanel>
        )}
      </div> 

      {/* This UI is now non-functional because we removed the allowlist, but it won't crash */}
      <div 
        className={`absolute inset-0 z-50 flex items-start justify-end p-4 transition-opacity duration-300
                    ${isInteractive ? 'pointer-events-none opacity-0' : 'pointer-events-auto opacity-100'}`}
        onClick={() => setIsInteractive(true)} 
      >
        {!isInteractive && (
          <button
            className="px-4 py-2 bg-glow-secondary/70 text-white rounded-lg shadow-lg hover:bg-glow-secondary focus:outline-none"
            onClick={() => setIsInteractive(true)}
          >
            Click to Re-Engage A.A.R.I.A.
          </button>
        )}
      </div>

      <button 
        className="absolute bottom-4 right-4 z-40 px-3 py-2 bg-gray-700/50 text-white rounded-full text-xs hover:bg-gray-600/70 transition-colors"
        onClick={() => setIsInteractive(!isInteractive)}
      >
        {isInteractive ? 'Hide A.A.R.I.A. (Click Through)' : 'Show A.A.R.I.A. (Engage)'}
      </button>
    </div>
  );
}

export default App;