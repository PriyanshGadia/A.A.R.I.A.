import { useState, FormEvent } from 'react';

interface TOTPScreenProps {
  onVerify: (code: string) => void;
  error: string | null;
  isConnected: boolean;
}

export function TOTPScreen({ onVerify, error, isConnected }: TOTPScreenProps) {
  const [code, setCode] = useState("");

  const handleSubmit = (e: FormEvent) => {
    e.preventDefault();
    if (!code.trim()) return;
    onVerify(code);
  };

  return (
    <div className="relative z-20 w-screen h-screen bg-base-bg flex flex-col items-center justify-center">
      <div className="w-full max-w-sm backdrop-blur-md bg-panel-bg border border-panel-border rounded-lg shadow-glow p-8">
        <h1 className="text-2xl font-bold text-center text-glow-secondary mb-4">A.A.R.I.A.</h1>
        <h2 className="text-lg font-medium text-center text-text-primary mb-6">Owner Verification Required</h2>

        <form onSubmit={handleSubmit} className="flex flex-col gap-4">
          <input
            type="text"
            value={code}
            onChange={(e) => setCode(e.target.value)}
            className="flex-grow bg-base-bg border border-panel-border rounded px-3 py-2 text-center text-2xl tracking-widest focus:outline-none focus:border-glow-secondary"
            placeholder="• • • • • •"
            maxLength={6}
            disabled={!isConnected}
          />
          <button type="submit" className="bg-glow-secondary text-white px-4 py-2 rounded disabled:opacity-50 font-bold" disabled={!isConnected}>
            {isConnected ? "VERIFY" : "Connecting..."}
          </button>

          {error && (
            <p className="text-center text-glow-accent mt-2">{error}</p>
          )}
        </form>
      </div>
    </div>
  );
}