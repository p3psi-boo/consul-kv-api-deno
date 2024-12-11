import { Hono } from 'hono'
import { HTTPException } from 'hono/http-exception'

type Session = {
  id: string
  name: string
  behavior: 'delete' | 'release'
  ttl: number
  createIndex: number
  lastRenewTime: string
}

type KVPair = {
  createIndex: number
  flags: number
  key: string
  lockIndex: number
  modifyIndex: number
  value: string
  session?: string
}

// 初始化 Deno KV store 替代内存存储
const kv = await Deno.openKv()
let consulIndex = 1

const app = new Hono()

// Middleware to handle Consul token authentication
app.use('*', async (c, next) => {
  const token = c.req.header('X-Consul-Token')
  if (!token) {
    // For demo purposes, we're not enforcing token
    // In production, you'd validate the token
  }
  await next()
})

// Get datacenters
app.get('/v1/catalog/datacenters', (c) => {
  // For demo purposes, returning a single DC
  return c.json(['dc1'])
})

// Create session
app.put('/v1/session/create', async (c) => {
  const body = await c.req.json()
  const sessionId = crypto.randomUUID()

  const session: Session = {
    id: sessionId,
    name: body.Name,
    behavior: body.Behavior.toLowerCase(),
    ttl: parseInt(body.TTL),
    createIndex: ++consulIndex,
    lastRenewTime: new Date().toISOString()
  }

  // 使用 KV store 存储 session
  await kv.set(['sessions', sessionId], session)

  return c.json({ ID: sessionId })
})

// Renew session
app.put('/v1/session/renew/:sessionId', async (c) => {
  const { sessionId } = c.req.param()
  const sessionRes = await kv.get<Session>(['sessions', sessionId])

  if (!sessionRes.value) {
    throw new HTTPException(404, { message: 'Session not found' })
  }

  const session = sessionRes.value
  session.lastRenewTime = new Date().toISOString()
  await kv.set(['sessions', sessionId], session)

  return c.json([session])
})

// Destroy session
app.put('/v1/session/destroy/:sessionId', async (c) => {
  const { sessionId } = c.req.param()
  const sessionRes = await kv.get<Session>(['sessions', sessionId])

  if (!sessionRes.value) {
    throw new HTTPException(404, { message: 'Session not found' })
  }

  const session = sessionRes.value
  await kv.delete(['sessions', sessionId])

  // 如果 session behavior 是 delete,删除所有被此 session 锁定的 KV entries
  if (session.behavior === 'delete') {
    const iter = kv.list<KVPair>({ prefix: ['kv'] })
    for await (const entry of iter) {
      if (entry.value.session === sessionId) {
        await kv.delete(entry.key)
      }
    }
  }

  return c.json(true)
})

// KV Store operations
// Get key
app.get('/v1/kv/*', async (c) => {
  const key = c.req.path.replace('/v1/kv/', '')
  const recurse = c.req.query('recurse') === 'true'
  const index = c.req.query('index')
  const wait = c.req.query('wait')
  const stale = c.req.query('stale') === 'true'

  // 处理递归获取
  if (recurse) {
    const prefix = key
    const results: KVPair[] = []
    const iter = kv.list<KVPair>({ prefix: ['kv', prefix] })

    for await (const entry of iter) {
      if (entry.value) {
        results.push(entry.value)
      }
    }

    if (results.length === 0) {
      return c.json([], 404)
    }

    c.header('X-Consul-Index', consulIndex.toString())
    return c.json(results)
  }

  // 处理单个 key 获取
  const value = await kv.get<KVPair>(['kv', key])
  if (!value.value) {
    return c.json([], 404)
  }

  // 处理阻塞查询
  if (index && parseInt(index) === value.value.modifyIndex && !stale) {
    // 等待指定时间
    if (wait) {
      await new Promise(resolve => setTimeout(resolve, parseInt(wait)))
    }
    return c.json([value.value])
  }

  c.header('X-Consul-Index', consulIndex.toString())
  return c.json([value.value])
})

// Put key
app.put('/v1/kv/*', async (c) => {
  const key = c.req.path.replace('/v1/kv/', '')
  const sessionId = c.req.query('acquire') || c.req.query('release')
  const flags = parseInt(c.req.query('flags') || '0')
  const cas = c.req.query('cas')
  const value = await c.req.text()

  // 检查 session
  if (sessionId) {
    const session = await kv.get<Session>(['sessions', sessionId])
    if (!session.value) {
      throw new HTTPException(400, { message: 'Invalid session' })
    }

    const existing = await kv.get<KVPair>(['kv', key])
    if (existing.value?.session && existing.value.session !== sessionId) {
      return c.json(false)
    }
  }

  // 检查 CAS
  if (cas) {
    const existing = await kv.get<KVPair>(['kv', key])
    if (existing.value?.modifyIndex !== parseInt(cas)) {
      return c.json(false)
    }
  }

  const kvPair: KVPair = {
    createIndex: ++consulIndex,
    modifyIndex: consulIndex,
    lockIndex: 0,
    flags,
    key,
    value,
    session: sessionId
  }

  await kv.set(['kv', key], kvPair)
  return c.json(true)
})

// Delete key
app.delete('/v1/kv/*', async (c) => {
  const key = c.req.path.replace('/v1/kv/', '')
  await kv.delete(['kv', key])
  return c.json(true)
})



Deno.serve(app.fetch)
