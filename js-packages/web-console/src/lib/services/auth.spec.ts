/**
 * Unit tests for the auth request/response middleware (run with `bun run test-unit`).
 *
 * These cover the token-refresh behaviour that keeps a session alive: the
 * request path must actively obtain a valid token, and the 401 retry must send
 * the *refreshed* token rather than the stale one captured before the 401.
 */
import { beforeEach, describe, expect, it, vi } from 'vitest'

// Controllable stand-in for the @axa-fr OidcClient singleton. `OidcClient.get()`
// throws when not initialized (mirroring the real library), which the code under
// test treats as "unauthenticated".
const mockState: { client: any } = { client: null }

vi.mock('@axa-fr/oidc-client', () => ({
  OidcClient: {
    get: () => {
      if (!mockState.client) {
        throw new Error('OidcClient not initialized')
      }
      return mockState.client
    }
  }
}))

import { applyAuthToRequest, handleAuthResponse } from './auth'

beforeEach(() => {
  mockState.client = null
  vi.clearAllMocks()
})

describe('applyAuthToRequest', () => {
  it('sets the Authorization header from the freshly validated token', async () => {
    mockState.client = {
      get tokens() {
        return { accessToken: 'stale' }
      },
      getValidTokenAsync: vi.fn(async () => ({
        isTokensValid: true,
        tokens: { accessToken: 'fresh' }
      }))
    }
    const out = await applyAuthToRequest(new Request('https://example.com/api'))
    expect(out.headers.get('Authorization')).toBe('Bearer fresh')
    expect(mockState.client.getValidTokenAsync).toHaveBeenCalledTimes(1)
  })

  it('falls back to the in-memory token when renewal throws (e.g. offline)', async () => {
    mockState.client = {
      get tokens() {
        return { accessToken: 'in-memory' }
      },
      getValidTokenAsync: vi.fn(async () => {
        throw new Error('offline')
      })
    }
    const out = await applyAuthToRequest(new Request('https://example.com/api'))
    expect(out.headers.get('Authorization')).toBe('Bearer in-memory')
  })

  it('leaves the request unauthenticated when the client is not initialized', async () => {
    mockState.client = null
    const out = await applyAuthToRequest(new Request('https://example.com/api'))
    expect(out.headers.get('Authorization')).toBeNull()
  })
})

describe('handleAuthResponse', () => {
  it('retries a 401 with the refreshed token, not the stale one', async () => {
    let currentToken = 'expired'
    mockState.client = {
      get tokens() {
        return { accessToken: currentToken }
      },
      getValidTokenAsync: vi.fn(async () => {
        currentToken = 'refreshed' // simulate a successful refresh-token exchange
        return { isTokensValid: true, tokens: { accessToken: currentToken } }
      })
    }
    // The request still carries the Authorization header that produced the 401.
    const request = new Request('https://example.com/api', {
      headers: { Authorization: 'Bearer expired' }
    })
    const response = new Response(null, { status: 401 })
    const fetchFn = vi.fn(async (_req: Request) => new Response('ok', { status: 200 }))

    const result = await handleAuthResponse(response, request, fetchFn as any)

    expect(result.status).toBe(200)
    expect(fetchFn).toHaveBeenCalledTimes(1)
    const retried = fetchFn.mock.calls[0][0] as Request
    expect(retried.headers.get('Authorization')).toBe('Bearer refreshed')
  })

  it('returns the original 401 when the token cannot be refreshed', async () => {
    mockState.client = {
      get tokens() {
        return { accessToken: 'expired' }
      },
      getValidTokenAsync: vi.fn(async () => ({ isTokensValid: false, tokens: null }))
    }
    const request = new Request('https://example.com/api')
    const response = new Response(null, { status: 401 })
    const fetchFn = vi.fn(async (_req: Request) => new Response('ok', { status: 200 }))

    const result = await handleAuthResponse(response, request, fetchFn as any)

    expect(result).toBe(response)
    expect(fetchFn).not.toHaveBeenCalled()
  })

  it('passes through non-401 responses untouched', async () => {
    const request = new Request('https://example.com/api')
    const response = new Response('boom', { status: 500 })
    const fetchFn = vi.fn(async (_req: Request) => new Response('ok', { status: 200 }))

    const result = await handleAuthResponse(response, request, fetchFn as any)

    expect(result).toBe(response)
    expect(fetchFn).not.toHaveBeenCalled()
  })
})
