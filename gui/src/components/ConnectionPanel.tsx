import React, { useState } from "react";
import { Server, Loader } from "lucide-react";
import { dbDriver, ConnectionConfig } from "../services/DatabaseDriver";

interface ConnectionPanelProps {
  onConnectionChange: (connected: boolean) => void;
}

export const ConnectionPanel: React.FC<ConnectionPanelProps> = ({ onConnectionChange }) => {
  const [config, setConfig] = useState<ConnectionConfig>({
    host: "127.0.0.1",
    port: 3282,
    database: "test",
    username: "",
    password: "",
  });
  const [isConnecting, setIsConnecting] = useState(false);

  const handleConnect = async () => {
    setIsConnecting(true);
    try {
      const success = await dbDriver.connect(config);
      onConnectionChange(success);
    } finally {
      setIsConnecting(false);
    }
  };

  const handleDisconnect = async () => {
    await dbDriver.disconnect();
    onConnectionChange(false);
  };

  const isConnected = dbDriver.getConnectionStatus();

  return (
    <div className="connection-panel">
      <div className="panel-header">
        <Server size={20} />
        <h3>Database Connection</h3>
      </div>
      
      <div className="connection-form">
        <div className="form-row">
          <input
            type="text"
            placeholder="Host"
            value={config.host}
            onChange={(e) => setConfig({ ...config, host: e.target.value })}
            disabled={isConnected}
          />
          <input
            type="number"
            placeholder="Port"
            value={config.port}
            onChange={(e) => setConfig({ ...config, port: parseInt(e.target.value) || 3282 })}
            disabled={isConnected}
          />
        </div>
        
        <input
          type="text"
          placeholder="Database (not implemented)"
          value={config.database}
          onChange={(e) => setConfig({ ...config, database: e.target.value })}
          disabled={true}
          className="disabled-input"
        />
        
        <input
          type="text"
          placeholder="Username (not implemented)"
          value={config.username}
          onChange={(e) => setConfig({ ...config, username: e.target.value })}
          disabled={true}
          className="disabled-input"
        />
        
        <input
          type="password"
          placeholder="Password (not implemented)"
          value={config.password}
          onChange={(e) => setConfig({ ...config, password: e.target.value })}
          disabled={true}
          className="disabled-input"
        />
        
        <button
          onClick={isConnected ? handleDisconnect : handleConnect}
          disabled={isConnecting}
          className={`connect-button ${isConnected ? 'disconnect' : 'connect'}`}
        >
          {isConnecting ? <Loader size={16} className="spinning" /> : null}
          {isConnected ? 'Disconnect' : 'Connect'}
        </button>
      </div>
    </div>
  );
};
