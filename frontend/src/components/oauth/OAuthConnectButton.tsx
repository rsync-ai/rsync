'use client';

import React, { useState, useRef, useEffect } from 'react';
import { Button } from '@/components/ui/button';
import { Loader2, ExternalLink, CheckCircle2, XCircle } from 'lucide-react';
import { cn } from '@/lib/utils';
import { API_ENDPOINTS, API_GATEWAY_URL } from '@/lib/config/api';
import { authFetch } from "@/lib/api/auth-fetch"

interface OAuthConnectButtonProps {
  provider: string;
  displayName: string;
  /** Override the idle-state button label. Defaults to "Connect {displayName}". */
  label?: string;
  className?: string;
  onSuccess?: (tokenId: string) => void;
  onError?: (error: string) => void;
  disabled?: boolean;
  variant?: 'default' | 'outline' | 'secondary';
  size?: 'default' | 'sm' | 'lg';
  /** Extra query params appended to the authorize URL (e.g. { shop: "mystore" } for Shopify). */
  extraParams?: Record<string, string>;
}

type ConnectionStatus = 'idle' | 'loading' | 'popup' | 'success' | 'error';

export function OAuthConnectButton({
  provider,
  displayName,
  label,
  className,
  onSuccess,
  onError,
  disabled = false,
  variant = 'default',
  size = 'default',
  extraParams,
}: OAuthConnectButtonProps) {
  const [status, setStatus] = useState<ConnectionStatus>('idle');
  const [errorMessage, setErrorMessage] = useState<string | null>(null);

  // The OAuth flow sets up an interval, window listeners, a BroadcastChannel and
  // several timers inside handleConnect. If the modal is closed (this component
  // unmounts) while the popup is still open, those keep running and call
  // setState on an unmounted component. Track liveness + the active teardown so
  // the unmount effect can tear everything down and guard every async setState.
  const mountedRef = useRef(true);
  const cleanupRef = useRef<(() => void) | null>(null);
  const timersRef = useRef<ReturnType<typeof setTimeout>[]>([]);
  useEffect(() => {
    return () => {
      mountedRef.current = false;
      cleanupRef.current?.();
      timersRef.current.forEach(clearTimeout);
      timersRef.current = [];
    };
  }, []);
  const setStatusSafe = (s: React.SetStateAction<ConnectionStatus>) => {
    if (mountedRef.current) setStatus(s);
  };
  const setErrorSafe = (e: React.SetStateAction<string | null>) => {
    if (mountedRef.current) setErrorMessage(e);
  };
  // setTimeout that no-ops after unmount and is cancelled by the unmount effect.
  const later = (fn: () => void, ms: number) => {
    const id = setTimeout(() => {
      if (mountedRef.current) fn();
    }, ms);
    timersRef.current.push(id);
  };

  const handleConnect = async () => {
    setStatusSafe('loading');
    setErrorSafe(null);

    try {
      // Get authorization URL from backend
      let authorizeUrl = API_ENDPOINTS.OAUTH.AUTHORIZE(provider);
      if (extraParams && Object.keys(extraParams).length > 0) {
        const qs = new URLSearchParams(extraParams).toString();
        authorizeUrl = `${authorizeUrl}?${qs}`;
      }
      const response = await authFetch(authorizeUrl, {
        method: 'GET',
      });

      // Handle 404 - OAuth endpoint not configured
      if (response.status === 404) {
        throw new Error(
          `OAuth is not configured for ${displayName}. Please enter credentials manually or contact your administrator to set up OAuth.`
        );
      }

      if (!response.ok) {
        // Try to parse error, but handle HTML responses gracefully
        let errorMessage = 'Failed to get authorization URL';
        try {
          const error = await response.json();
          errorMessage = error.message || errorMessage;
        } catch {
          // Response is not JSON (likely HTML error page)
          errorMessage = `OAuth endpoint returned ${response.status}. OAuth may not be configured for this connector.`;
        }
        throw new Error(errorMessage);
      }

      const data = await response.json();
      const { authorization_url, state } = data;

      // Store state for verification
      sessionStorage.setItem('oauth_state', state);
      sessionStorage.setItem('oauth_provider', provider);

      // Open popup for OAuth flow
      setStatusSafe('popup');
      const popup = window.open(
        authorization_url,
        `oauth_${provider}`,
        'width=600,height=700,scrollbars=yes,resizable=yes'
      );

      if (!popup) {
        throw new Error('Popup blocked. Please allow popups for this site.');
      }

      // The OAuth result is delivered over THREE channels because identity
      // providers like Shopify set Cross-Origin-Opener-Policy on their
      // authorize page, which severs window.opener — so the classic
      // window.opener.postMessage from the callback never arrives. The callback
      // page also broadcasts over a same-origin BroadcastChannel and writes to
      // localStorage; both survive the opener being severed. We listen on all
      // three and de-dupe with `settled`.
      let settled = false;
      let bc: BroadcastChannel | null = null;

      const cleanup = () => {
        window.removeEventListener('message', handleMessage);
        window.removeEventListener('storage', handleStorage);
        try { bc?.close(); } catch { /* noop */ }
        clearInterval(checkClosed);
      };
      // Expose to the unmount effect so closing the modal mid-flow tears this down.
      cleanupRef.current = cleanup;

      // Validate + act on a result payload regardless of which channel it came from.
      const handleResult = (data: Record<string, unknown>) => {
        if (settled) return;

        const expectedState = sessionStorage.getItem('oauth_state');
        const expectedProvider = sessionStorage.getItem('oauth_provider');

        const type = data.type as string | undefined;
        const providerFromMsg = data.provider as string | undefined;
        const stateFromMsg = data.state as string | undefined;
        const tokenId = (data.tokenId ?? data.token_id) as string | undefined;
        const error = data.error as string | undefined;

        if (!type) return;
        // The state is a CSRF-grade random token minted per authorize call, so a
        // provider+state match is sufficient identity proof on same-origin channels.
        if (!expectedProvider || !expectedState) return;
        if (providerFromMsg !== expectedProvider) return;
        if (stateFromMsg !== expectedState) return;

        if (type === 'oauth_success') {
          settled = true;
          cleanup();
          setStatusSafe('success');

          sessionStorage.removeItem('oauth_state');
          sessionStorage.removeItem('oauth_provider');
          try { localStorage.removeItem('oauth_result'); } catch { /* noop */ }

          if (onSuccess && typeof tokenId === 'string' && tokenId.length > 0) {
            onSuccess(tokenId);
          }

          later(() => setStatusSafe('idle'), 2000);
        } else if (type === 'oauth_error') {
          settled = true;
          cleanup();
          setStatusSafe('error');
          setErrorSafe(error || 'OAuth authorization failed');

          if (onError) {
            onError(error || 'OAuth authorization failed');
          }

          later(() => {
            setStatusSafe('idle');
            setErrorSafe(null);
          }, 3000);
        }
      };

      // Channel 1: classic postMessage from the popup (opener intact).
      const handleMessage = (event: MessageEvent) => {
        const apiGatewayOrigin = new URL(API_GATEWAY_URL).origin;
        const allowedOrigins = [window.location.origin, apiGatewayOrigin];
        if (!allowedOrigins.includes(event.origin)) return;
        // NOTE: do NOT require event.source === popup — COOP on the provider's
        // authorize page nulls the opener/source relationship.
        handleResult((event.data || {}) as Record<string, unknown>);
      };

      // Channel 2: localStorage 'storage' event (same-origin, survives COOP).
      const handleStorage = (event: StorageEvent) => {
        if (event.key !== 'oauth_result' || !event.newValue) return;
        try {
          handleResult(JSON.parse(event.newValue) as Record<string, unknown>);
        } catch { /* ignore malformed payloads */ }
      };

      // Channel 3: BroadcastChannel (same-origin, survives COOP).
      try {
        bc = new BroadcastChannel('oauth_result');
        bc.onmessage = (ev) => handleResult((ev.data || {}) as Record<string, unknown>);
      } catch { /* BroadcastChannel unsupported — postMessage/storage still cover it */ }

      window.addEventListener('message', handleMessage);
      window.addEventListener('storage', handleStorage);

      // Detect a popup the user closed without completing. We can't read
      // popup.closed reliably once COOP severs the handle, so guard the access.
      const checkClosed = setInterval(() => {
        let closed = false;
        try { closed = !!popup && popup.closed; } catch { closed = false; }
        if (closed) {
          if (!settled) {
            cleanup();
            setStatusSafe((s) => (s === 'popup' ? 'idle' : s));
          }
        }
      }, 500);

    } catch (error) {
      console.error('OAuth error:', error);
      setStatusSafe('error');
      const message = error instanceof Error ? error.message : 'OAuth connection failed';
      setErrorSafe(message);

      if (onError) {
        onError(message);
      }

      later(() => {
        setStatusSafe('idle');
        setErrorSafe(null);
      }, 5000); // Longer timeout for error messages so users can read them
    }
  };

  const getButtonContent = () => {
    switch (status) {
      case 'loading':
        return (
          <>
            <Loader2 className="mr-2 h-4 w-4 animate-spin" />
            Connecting...
          </>
        );
      case 'popup':
        return (
          <>
            <ExternalLink className="mr-2 h-4 w-4" />
            Complete in popup...
          </>
        );
      case 'success':
        return (
          <>
            <CheckCircle2 className="mr-2 h-4 w-4 text-green-500" />
            Connected!
          </>
        );
      case 'error':
        return (
          <>
            <XCircle className="mr-2 h-4 w-4 text-red-500" />
            {errorMessage || 'Failed'}
          </>
        );
      default:
        return (
          <>
            <span className="mr-2">🔐</span>
            {label ?? `Connect ${displayName}`}
          </>
        );
    }
  };

  return (
    <div className="space-y-2">
      <Button
        variant={status === 'error' ? 'destructive' : variant}
        size={size}
        className={cn(className)}
        onClick={handleConnect}
        disabled={disabled || status === 'loading' || status === 'popup'}
      >
        {getButtonContent()}
      </Button>
      
      {status === 'error' && errorMessage && (
        <div className="text-sm text-amber-600 dark:text-amber-400 bg-amber-50 dark:bg-amber-950/30 p-3 rounded-lg border border-amber-200 dark:border-amber-800">
          <p className="font-medium mb-1">⚠️ OAuth Not Available</p>
          <p className="text-xs">{errorMessage}</p>
          <p className="text-xs mt-2 text-amber-700 dark:text-amber-300">
            💡 Tip: You can still create a connection by entering credentials manually below.
          </p>
        </div>
      )}
    </div>
  );
}

export default OAuthConnectButton;
