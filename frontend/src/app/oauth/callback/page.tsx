'use client';

import { Suspense, useEffect, useState } from 'react';
import { useSearchParams } from 'next/navigation';
import { Loader2, CheckCircle2, XCircle } from 'lucide-react';
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from '@/components/ui/card';

type CallbackStatus = 'processing' | 'success' | 'error';

function OAuthCallbackInner() {
  const searchParams = useSearchParams();
  const [status, setStatus] = useState<CallbackStatus>('processing');
  const [message, setMessage] = useState<string>('Processing OAuth callback...');
  const [tokenId, setTokenId] = useState<string | null>(null);

  useEffect(() => {
    handleCallback();
  }, []);

  const handleCallback = async () => {
    const stateParam = searchParams.get('state');
    const providerParam = sessionStorage.getItem('oauth_provider');
    try {
      // Get OAuth parameters from URL
      const code = searchParams.get('code');
      const state = stateParam;
      const error = searchParams.get('error');
      const errorDescription = searchParams.get('error_description');

      // Check for OAuth errors
      if (error) {
        throw new Error(errorDescription || error || 'OAuth authorization failed');
      }

      if (!code) {
        throw new Error('No authorization code received');
      }

      // Get stored state and provider from session storage
      const storedState = sessionStorage.getItem('oauth_state');
      const provider = providerParam;

      // Verify state (CSRF protection)
      if (!state || !storedState || state !== storedState) {
        throw new Error('Invalid state parameter - please start OAuth flow again');
      }

      if (!provider) {
        throw new Error('No provider found in session - please start OAuth flow again');
      }

      // Exchange code for token via our backend
      setMessage(`Exchanging code with ${provider}...`);

      const response = await fetch(`/api/v1/oauth/callback/${provider}?code=${code}&state=${state}`, {
        method: 'GET',
        headers: {
          'Content-Type': 'application/json',
        },
      });

      if (!response.ok) {
        const errorData = await response.json();
        throw new Error(errorData.message || 'Failed to exchange authorization code');
      }

      const data = await response.json();

      // Success!
      setStatus('success');
      setTokenId(data.token_id);
      setMessage(`Connected to ${provider} successfully!`);

      // Post message to parent window (if opened as popup)
      if (window.opener && !window.opener.closed) {
        window.opener.postMessage(
          {
            type: 'oauth_success',
            provider,
            state,
            tokenId: data.token_id,
            accountName: data.account_name,
          },
          window.location.origin
        );

        // Close popup after short delay
        setTimeout(() => {
          window.close();
        }, 1500);
      }

    } catch (error) {
      console.error('OAuth callback error:', error);
      setStatus('error');
      setMessage(error instanceof Error ? error.message : 'OAuth callback failed');

      // Post error to parent window
      if (window.opener && !window.opener.closed) {
        window.opener.postMessage(
          {
            type: 'oauth_error',
            provider: providerParam ?? undefined,
            state: stateParam ?? undefined,
            error: error instanceof Error ? error.message : 'Unknown error',
          },
          window.location.origin
        );
      }
    }
  };

  return (
    <div className="min-h-screen flex items-center justify-center bg-gradient-to-br from-gray-50 to-gray-100 dark:from-gray-900 dark:to-gray-800">
      <Card className="w-full max-w-md mx-4">
        <CardHeader className="text-center">
          <CardTitle className="flex items-center justify-center gap-2">
            {status === 'processing' && (
              <>
                <Loader2 className="h-6 w-6 animate-spin text-blue-500" />
                Connecting...
              </>
            )}
            {status === 'success' && (
              <>
                <CheckCircle2 className="h-6 w-6 text-green-500" />
                Connected!
              </>
            )}
            {status === 'error' && (
              <>
                <XCircle className="h-6 w-6 text-red-500" />
                Connection Failed
              </>
            )}
          </CardTitle>
          <CardDescription>{message}</CardDescription>
        </CardHeader>
        <CardContent className="text-center">
          {status === 'success' && (
            <div className="space-y-4">
              {tokenId && (
                <p className="text-sm text-muted-foreground">
                  Token ID: <code className="bg-muted px-2 py-1 rounded">{tokenId}</code>
                </p>
              )}
              <p className="text-sm text-muted-foreground">
                {window.opener 
                  ? 'This window will close automatically...' 
                  : 'You can close this window and return to the application.'}
              </p>
            </div>
          )}
          {status === 'error' && (
            <div className="space-y-4">
              <p className="text-sm text-red-600 dark:text-red-400">
                Please try connecting again or contact support if the issue persists.
              </p>
              {!window.opener && (
                <a 
                  href="/connections" 
                  className="inline-block text-sm text-blue-600 hover:underline"
                >
                  Return to Connections
                </a>
              )}
            </div>
          )}
          {status === 'processing' && (
            <div className="py-4">
              <div className="w-full bg-gray-200 dark:bg-gray-700 rounded-full h-2">
                <div 
                  className="bg-blue-500 h-2 rounded-full animate-pulse" 
                  style={{ width: '60%' }}
                />
              </div>
            </div>
          )}
        </CardContent>
      </Card>
    </div>
  );
}

export default function OAuthCallbackPage() {
  // Next.js requires useSearchParams() usage to be wrapped in Suspense.
  return (
    <Suspense fallback={<div />}>
      <OAuthCallbackInner />
    </Suspense>
  );
}
