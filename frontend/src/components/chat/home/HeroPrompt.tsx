'use client';

import { useState, type RefObject } from 'react';
import { ArrowRight, Search, Sparkles } from 'lucide-react';
import { Button } from '@/components/ui/button';
import { Input } from '@/components/ui/input';
import { cn } from '@/lib/utils';

interface HeroPromptProps {
  inputRef: RefObject<HTMLInputElement | null>;
  examples: string[];
  onSubmit: (intent: string) => void;
}

export function HeroPrompt({ inputRef, examples, onSubmit }: HeroPromptProps) {
  const [intent, setIntent] = useState('');

  const handleSubmit = (e: React.FormEvent) => {
    e.preventDefault();
    if (intent.trim()) onSubmit(intent.trim());
  };

  return (
    <div className="w-full max-w-2xl mx-auto text-center">
      <div className="inline-flex items-center justify-center w-14 h-14 rounded-2xl bg-gradient-to-br from-violet-500 to-purple-600 text-white mb-4 shadow-lg shadow-violet-500/20">
        <Sparkles className="w-7 h-7" aria-hidden />
      </div>
      <h1 className="text-3xl font-bold tracking-tight text-gray-900 dark:text-white mb-2">
        Agentic Data Pipeline
      </h1>
      <p className="text-gray-500 dark:text-gray-400 max-w-md mx-auto mb-6">
        Describe your data pipeline in plain English, or start from a suggestion below
      </p>

      <form onSubmit={handleSubmit}>
        <div
          className={cn(
            'relative flex items-center rounded-2xl border-2 bg-white dark:bg-gray-900 shadow-sm transition-all',
            'border-gray-200 dark:border-gray-700 hover:border-gray-300 dark:hover:border-gray-600',
            'focus-within:border-violet-500 focus-within:shadow-lg focus-within:shadow-violet-100 dark:focus-within:shadow-violet-900/20'
          )}
        >
          <Search className="absolute left-4 w-5 h-5 text-gray-400" aria-hidden />
          <Input
            ref={inputRef}
            type="text"
            value={intent}
            onChange={(e) => setIntent(e.target.value)}
            aria-label="Describe your pipeline"
            placeholder='Try "sync mysql to s3" or "backup postgres daily"'
            className="pl-12 pr-16 py-6 text-base md:text-lg border-0 shadow-none focus-visible:ring-0 rounded-2xl bg-transparent"
          />
          <Button
            type="submit"
            disabled={!intent.trim()}
            aria-label="Start pipeline chat"
            className={cn(
              'absolute right-2 rounded-xl px-4 py-2 transition-all',
              intent.trim()
                ? 'bg-violet-600 hover:bg-violet-700 text-white'
                : 'bg-gray-100 dark:bg-gray-800 text-gray-400'
            )}
          >
            <ArrowRight className="w-5 h-5" />
          </Button>
        </div>
      </form>

      {examples.length > 0 && (
        <div className="mt-3 flex flex-wrap items-center justify-center gap-2">
          <span className="text-xs text-gray-400 dark:text-gray-500">Try:</span>
          {examples.map((example) => (
            <button
              key={example}
              type="button"
              onClick={() => {
                // Prefill rather than send, so the user can edit before starting.
                setIntent(example);
                inputRef.current?.focus();
              }}
              className="rounded-full border border-gray-200 dark:border-gray-700 bg-white dark:bg-gray-900 px-3 py-1 text-xs text-gray-600 dark:text-gray-300 hover:border-violet-300 hover:text-violet-700 dark:hover:border-violet-600 dark:hover:text-violet-300 transition-colors focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-violet-500"
            >
              {example}
            </button>
          ))}
        </div>
      )}
    </div>
  );
}
