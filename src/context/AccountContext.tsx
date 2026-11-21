import React, { createContext, useCallback, useContext, useEffect, useRef, useState } from 'react';
import { supabase } from '../lib/supabase';
import {
  applySimEvent, balancesFromRows, cents, getSimClientId, loadSimQueue,
  reconcileSimQueue, saveSimQueue, simModes, startingBalances,
} from '../utils/simBalanceSync';
import type { NewSimBalanceEvent, StoredSimQueue } from '../utils/simBalanceSync';

export interface AccountBalances {
  currency: string;
  totalUsd: number;
  cfdsUsd: number;
  optionsUsd: number;
  walletUsd: number;
  walletUsdt: number;
  p2pUsd: number;
  lastUpdated: string;
}
export type AccountMode = 'real' | 'demo';
export type AccountKey = 'cfdsUsd' | 'optionsUsd' | 'walletUsd' | 'walletUsdt' | 'p2pUsd';
export interface AccountContextType {
  activeMode: AccountMode;
  setActiveMode: (mode: AccountMode) => void;
  balances: AccountBalances;
  realBalances: AccountBalances;
  demoBalances: AccountBalances;
  updateOptionsBalance: (deltaOrValue: number, isAbsolute?: boolean) => boolean;
  adminOverrideBalances: (realUpdates: Partial<AccountBalances>, demoUpdates?: Partial<AccountBalances>) => void;
  transferFunds: (fromKey: AccountKey, toKey: AccountKey, amount: number) => boolean;
  flushPending: () => void;
  syncError: string | null;
  pendingCount: number;
  winAccuracy: number;
  setWinAccuracy: (accuracy: number) => void;
}

const AccountContext = createContext<AccountContextType | undefined>(undefined);
type Queues = Record<AccountMode, StoredSimQueue>;
type ModesBalances = Record<AccountMode, AccountBalances>;

export const AccountProvider: React.FC<{ userId: string; children: React.ReactNode }> = ({ userId, children }) => {
  const [activeMode, setActiveMode] = useState<AccountMode>('real');
  const [winAccuracy, setWinAccuracy] = useState(60);
  const [realBalances, setRealBalances] = useState(startingBalances);
  const [demoBalances, setDemoBalances] = useState(startingBalances);
  const balancesRef = useRef<ModesBalances>({ real: startingBalances(), demo: startingBalances() });
  const queuesRef = useRef<Queues | null>(null);
  const clientIdRef = useRef('');
  const readyRef = useRef(false);
  const inFlightRef = useRef<Record<AccountMode, boolean>>({ real: false, demo: false });
  const [ready, setReady] = useState(false);
  const [initialError, setInitialError] = useState<string | null>(null);
  const [syncError, setSyncError] = useState<string | null>(null);
  const [pendingCount, setPendingCount] = useState(0);
  const [retry, setRetry] = useState(0);

  const publish = useCallback((mode: AccountMode, next: AccountBalances) => {
    balancesRef.current[mode] = next;
    if (mode === 'real') setRealBalances(next);
    else setDemoBalances(next);
  }, []);

  const updatePendingCount = useCallback(() => {
    const queues = queuesRef.current;
    setPendingCount(queues ? queues.real.events.length + queues.demo.events.length : 0);
  }, []);

  useEffect(() => {
    let cancelled = false;
    readyRef.current = false;
    queuesRef.current = null;
    setReady(false);
    setInitialError(null);
    const initialize = async () => {
      try {
        const clientId = getSimClientId(userId);
        const localReal = loadSimQueue(userId, 'real', clientId);
        const localDemo = loadSimQueue(userId, 'demo', clientId);
        const { data, error } = await supabase.rpc('dclone_sim_snapshot', { p_client_id: clientId });
        if (error) throw error;
        if (cancelled) return;
        const snapshot = data as {
          balances: unknown; cursors: Record<AccountMode, number>;
        };
        const resolved: Queues = {
          real: reconcileSimQueue(localReal, Number(snapshot.cursors?.real)),
          demo: reconcileSimQueue(localDemo, Number(snapshot.cursors?.demo)),
        };
        const calculated: ModesBalances = {
          real: balancesFromRows(snapshot.balances, 'real'),
          demo: balancesFromRows(snapshot.balances, 'demo'),
        };
        for (const mode of simModes) {
          for (const event of resolved[mode].events) calculated[mode] = applySimEvent(calculated[mode], event);
        }
        // Keep unacknowledged changes on disk, excluding those the server confirms.
        saveSimQueue(userId, 'real', resolved.real);
        saveSimQueue(userId, 'demo', resolved.demo);
        clientIdRef.current = clientId;
        queuesRef.current = resolved;
        publish('real', calculated.real);
        publish('demo', calculated.demo);
        readyRef.current = true;
        setReady(true);
        setSyncError(null);
        updatePendingCount();
      } catch (error) {
        if (!cancelled) setInitialError(error instanceof Error ? error.message : 'Unable to initialize balances');
      }
    };
    // One active tab per browser profile: localStorage is not a transactional multi-tab queue.
    // The Web Locks API prevents two tabs issuing the same client sequence.
    let releaseLock: (() => void) | null = null;
    if (navigator.locks?.request) {
      void navigator.locks.request('dclone-sim-wallet:' + userId, { mode: 'exclusive', ifAvailable: true }, async lock => {
        if (!lock) {
          if (!cancelled) setInitialError('This simulated account is open in another tab. Close that tab and try again.');
          return;
        }
        if (cancelled) return;
        await initialize();
        await new Promise<void>(resolve => {
          releaseLock = resolve;
          if (cancelled) resolve();
        });
      }).catch(error => {
        if (!cancelled) setInitialError(error instanceof Error ? error.message : 'Unable to lock the account');
      });
    } else {
      // Older browsers lack Web Locks. Use only one tab on those browsers.
      void initialize();
    }
    return () => {
      cancelled = true;
      readyRef.current = false;
      releaseLock?.();
    };
  }, [userId, retry, publish, updatePendingCount]);

  const record = useCallback((mode: AccountMode, event: NewSimBalanceEvent): boolean => {
    const queues = queuesRef.current;
    if (!readyRef.current || !queues) return false;
    const old = queues[mode];
    if (old.events.length >= 5000) {
      setSyncError('Too many unsent simulated changes. Trading paused until they sync.');
      return false;
    }
    const entry = { ...event, seq: old.lastIssued + 1 };
    try {
      const nextBalances = applySimEvent(balancesRef.current[mode], entry);
      const nextQueue: StoredSimQueue = {
        ...old, lastIssued: entry.seq, events: [...old.events, entry],
      };
      // Persist first, so no trade is ever shown without a locally stored change.
      saveSimQueue(userId, mode, nextQueue);
      queues[mode] = nextQueue;
      publish(mode, nextBalances);
      updatePendingCount();
      return true;
    } catch (error) {
      setSyncError(error instanceof Error ? error.message : 'Unable to save pending balance change');
      return false;
    }
  }, [userId, publish, updatePendingCount]);

  const flushMode = useCallback(async (mode: AccountMode) => {
    const queues = queuesRef.current;
    if (!readyRef.current || !queues || inFlightRef.current[mode]) return;
    const batch = queues[mode].events.slice(0, 500);
    if (!batch.length) return;
    inFlightRef.current[mode] = true;
    try {
      const { data, error } = await supabase.rpc('dclone_sim_apply_batch', {
        p_client_id: clientIdRef.current, p_mode: mode, p_events: batch,
      });
      if (error) throw error;
      if (!readyRef.current || !queuesRef.current) return;
      const response = data as { last_sequence: number; balances: unknown };
      const confirmed = Number(response.last_sequence);
      if (!Number.isSafeInteger(confirmed) || confirmed < batch[batch.length - 1].seq) {
        throw new Error('Database did not acknowledge the complete batch');
      }
      const nextQueue = reconcileSimQueue(queuesRef.current[mode], confirmed);
      const base = balancesFromRows(response.balances, mode);
      const nextView = nextQueue.events.reduce(applySimEvent, base);
      // If this write fails, the server cursor still makes retries harmless.
      saveSimQueue(userId, mode, nextQueue);
      queuesRef.current[mode] = nextQueue;
      publish(mode, nextView);
      updatePendingCount();
      setSyncError(null);
    } catch (error) {
      if (readyRef.current) {
        setSyncError('Balance sync pending: ' + (error instanceof Error ? error.message : String(error)));
      }
    } finally {
      inFlightRef.current[mode] = false;
    }
  }, [userId, publish, updatePendingCount]);

  const flushPending = useCallback(() => {
    void flushMode('real');
    void flushMode('demo');
  }, [flushMode]);

  useEffect(() => {
    if (!ready) return;
    const interval = window.setInterval(flushPending, 1000);
    const onVisibilityChange = () => { if (document.visibilityState === 'hidden') flushPending(); };
    document.addEventListener('visibilitychange', onVisibilityChange);
    flushPending(); // Recover the durable queue immediately after login.
    return () => {
      window.clearInterval(interval);
      document.removeEventListener('visibilitychange', onVisibilityChange);
    };
  }, [ready, flushPending]);

  const updateOptionsBalance = (deltaOrValue: number, isAbsolute = false): boolean => {
    try {
      const value = cents(deltaOrValue);
      return record(activeMode, isAbsolute
        ? { kind: 'set', account: 'optionsUsd', cents: value }
        : { kind: 'adjust', account: 'optionsUsd', cents: value });
    } catch { return false; }
  };

  const transferFunds = (fromKey: AccountKey, toKey: AccountKey, amount: number): boolean => {
    if (fromKey === toKey || amount <= 0) return false;
    try {
      return record(activeMode, { kind: 'transfer', from: fromKey, to: toKey, amount_cents: cents(amount) });
    } catch { return false; }
  };

  const adminOverrideBalances = (
    realUpdates: Partial<AccountBalances>, demoUpdates: Partial<AccountBalances> = {},
  ) => {
    for (const [mode, updates] of [['real', realUpdates], ['demo', demoUpdates]] as const) {
      for (const account of ['optionsUsd', 'cfdsUsd', 'walletUsd', 'walletUsdt', 'p2pUsd'] as AccountKey[]) {
        const value = updates[account];
        if (value !== undefined) {
          try { record(mode, { kind: 'set', account, cents: cents(value) }); }
          catch { setSyncError('Invalid simulated admin balance'); }
        }
      }
    }
  };

  const balances = activeMode === 'real' ? realBalances : demoBalances;

  if (!ready) {
    return (
      <div role="status" style={{ minHeight: '100vh', display: 'grid', placeItems: 'center', padding: 24, textAlign: 'center' }}>
        {initialError ? (
          <div>
            <p>Could not load your simulated balances. No pending changes were discarded.</p>
            <p>{initialError}</p>
            <button type="button" onClick={() => setRetry(value => value + 1)}>Try again</button>
          </div>
        ) : <p>Loading your simulated account balances…</p>}
      </div>
    );
  }

  return (
    <AccountContext.Provider value={{
      activeMode, setActiveMode, balances, realBalances, demoBalances,
      updateOptionsBalance, adminOverrideBalances, transferFunds, flushPending,
      syncError, pendingCount, winAccuracy, setWinAccuracy,
    }}>
      {children}
      {syncError && (
        <div role="status" style={{
          position: 'fixed', left: 12, right: 12, bottom: 12, zIndex: 99999,
          background: '#fff3cd', color: '#694a00', padding: '8px 12px',
          borderRadius: 8, fontSize: 12, boxShadow: '0 1px 8px #0002',
        }}>
          {syncError} · {pendingCount} change(s) saved locally
        </div>
      )}
    </AccountContext.Provider>
  );
};

export const useAccount = () => {
  const ctx = useContext(AccountContext);
  if (!ctx) throw new Error('useAccount must be used within an AccountProvider');
  return ctx;
};
