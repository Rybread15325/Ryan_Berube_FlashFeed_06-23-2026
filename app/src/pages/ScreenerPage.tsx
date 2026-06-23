'use client'
import useSWR from 'swr'
import { useState, useMemo } from 'react'
import { ScreenerTable } from './ScreenerTable'
import { ScreenerFilterPanel } from './ScreenerFilterPanel'
import { IntradayChart } from './IntradayChart'
import type { Article, ScreenerRow } from '@/lib/types'

const fetcher = (url: string) => fetch(url).then(r => r.json())

export type ViewMode = 'overview' | 'performance' | 'technical' | 'sentiment'
type FilterTab = 'descriptive' | 'technical' | 'performance' | 'sentiment' | 'all'

const VIEW_MODES: ViewMode[] = ['overview', 'performance', 'technical', 'sentiment']
const PRESETS = [
  { key: '', label: 'All' },
  { key: 'top_gainers', label: 'Top Gainers' },
  { key: 'top_losers', label: 'Top Losers' },
  { key: 'unusual_volume', label: 'Unusual Volume' },
  { key: 'bullish_news', label: 'Bullish News' },
  { key: 'bearish_news', label: 'Bearish News' },
  { key: 'oversold', label: 'Oversold' },
  { key: 'overbought', label: 'Overbought' },
]

const MARKET_CAP_OPTIONS = [
  { value: '', label: 'Any' },
  { value: 'micro', label: 'Micro (<300M)' },
  { value: 'small', label: 'Small (300M–2B)' },
  { value: 'mid', label: 'Mid (2B–10B)' },
  { value: 'large', label: 'Large (10B–200B)' },
  { value: 'mega', label: 'Mega (>200B)' },
]

function compact(n: number | null | undefined) {
  if (n == null || Number.isNaN(n)) return '--'
  if (Math.abs(n) >= 1e12) return `${(n / 1e12).toFixed(1)}T`
  if (Math.abs(n) >= 1e9) return `${(n / 1e9).toFixed(1)}B`
  if (Math.abs(n) >= 1e6) return `${(n / 1e6).toFixed(1)}M`
  if (Math.abs(n) >= 1e3) return `${(n / 1e3).toFixed(1)}K`
  return n.toLocaleString()
}

export function ScreenerPage() {
  const [socialWindow, setSocialWindow] = useState('adaptive')
  const screenerParams = new URLSearchParams({ limit: '1500' })
  if (socialWindow !== 'adaptive') screenerParams.set('window_minutes', socialWindow)
  const { data, isLoading, mutate } = useSWR(`/api/screener?${screenerParams.toString()}`, fetcher, { refreshInterval: 30_000 })
  const { data: newsData } = useSWR('/api/articles?mover_only=1&ticker_only=1&recent_days=2&limit=24', fetcher, { refreshInterval: 30_000 })
  const [filters, setFilters] = useState<Record<string, string>>({})
  const [showFilters, setShowFilters] = useState(false)
  const [filterTab, setFilterTab] = useState<FilterTab>('all')
  const [viewMode, setViewMode] = useState<ViewMode>('overview')
  const [signal, setSignal] = useState('')
  const [orderBy, setOrderBy] = useState('ticker')
  const [orderDir, setOrderDir] = useState<'asc' | 'desc'>('asc')
  const [search, setSearch] = useState('')
  const [page, setPage] = useState(0)
  const pageSize = 50

  const tickers: ScreenerRow[] = Array.isArray(data) ? data : data?.tickers ?? data?.rows ?? []

  const filtered = useMemo(() => {
    let rows = [...tickers].filter(t => (
      t.price != null &&
      t.change_pct != null &&
      ['NASDAQ', 'NYSE', 'AMEX'].includes(String((t as any).exchange || '').toUpperCase()) &&
      !String(t.ticker || '').includes('.')
    ))

    // Search
    if (search) {
      const q = search.toLowerCase()
      rows = rows.filter(t => t.ticker.toLowerCase().includes(q) || (t.company ?? '').toLowerCase().includes(q))
    }

    // Filters
    if (filters.sector) rows = rows.filter(t => t.sector === filters.sector)
    if (filters.exchange) rows = rows.filter(t => (t as any).exchange === filters.exchange)
    if (filters.index) rows = rows.filter(t => (t as any).index === filters.index)
    if (filters.country) rows = rows.filter(t => (t as any).country === filters.country)
    if (filters.industry) rows = rows.filter(t => t.industry === filters.industry)
    if (filters.market_cap) {
      const mc = filters.market_cap
      rows = rows.filter(t => {
        const cap = (t as any).market_cap ?? 0
        if (mc === 'micro') return cap < 300e6
        if (mc === 'small') return cap >= 300e6 && cap < 2e9
        if (mc === 'mid') return cap >= 2e9 && cap < 10e9
        if (mc === 'large') return cap >= 10e9 && cap < 200e9
        if (mc === 'mega') return cap >= 200e9
        return true
      })
    }
    if (filters.price_change) {
      const pc = filters.price_change
      rows = rows.filter(t => {
        const change = t.change_pct
        if (change == null) return false
        if (pc === 'up') return change > 0
        if (pc === 'down') return change < 0
        if (pc === 'up2') return change >= 2
        if (pc === 'up5') return change >= 5
        if (pc === 'up10') return change >= 10
        if (pc === 'down2') return change <= -2
        if (pc === 'down5') return change <= -5
        return true
      })
    }
    if (filters.avg_volume) {
      const av = parseInt(filters.avg_volume)
      rows = rows.filter(t => t.volume != null && t.volume >= av)
    }
    if (filters.rel_volume) {
      rows = rows.filter(t => {
        const rv = (t as any).rel_volume ?? 0
        if (filters.rel_volume === 'over1') return rv >= 1
        if (filters.rel_volume === 'over1_5') return rv >= 1.5
        if (filters.rel_volume === 'over2') return rv >= 2
        if (filters.rel_volume === 'over3') return rv >= 3
        return true
      })
    }
    if (filters.price_range) {
      const pr = filters.price_range
      rows = rows.filter(t => {
        const p = t.price
        if (p == null) return false
        if (pr === 'under1') return p < 1
        if (pr === 'under5') return p < 5
        if (pr === 'under10') return p < 10
        if (pr === 'under20') return p < 20
        if (pr === 'over5') return p >= 5
        if (pr === 'over10') return p >= 10
        if (pr === 'over20') return p >= 20
        if (pr === 'over50') return p >= 50
        if (pr === 'over100') return p >= 100
        return true
      })
    }
    if (filters.social_sentiment) {
      const ss = filters.social_sentiment
      rows = rows.filter(t => {
        const value = t.social_sentiment ?? 0
        if (ss === 'bullish') return value >= 0.2
        if (ss === 'bearish') return value <= -0.2
        if (ss === 'neutral') return value > -0.2 && value < 0.2
        return true
      })
    }
    if (filters.stocktwits_sentiment) {
      const ss = filters.stocktwits_sentiment
      rows = rows.filter(t => {
        const value = t.social_message_sentiment ?? 0
        if (ss === 'bullish') return value >= 0.2
        if (ss === 'bearish') return value <= -0.2
        if (ss === 'neutral') return value > -0.2 && value < 0.2
        return true
      })
    }
    if (filters.stocktwits_density) {
      rows = rows.filter(t => {
        const value = t.social_message_density ?? 0
        if (filters.stocktwits_density === 'over0_05') return value >= 0.05
        if (filters.stocktwits_density === 'over0_1') return value >= 0.1
        if (filters.stocktwits_density === 'over0_5') return value >= 0.5
        if (filters.stocktwits_density === 'over1') return value >= 1
        return true
      })
    }
    if (filters.news_sentiment) {
      const ns = filters.news_sentiment
      rows = rows.filter(t => {
        const value = t.structured_sentiment ?? 0
        if (ns === 'bullish') return value >= 0.2
        if (ns === 'bearish') return value <= -0.2
        if (ns === 'neutral') return value > -0.2 && value < 0.2
        return true
      })
    }
    if (filters.min_posts) {
      const mp = parseInt(filters.min_posts)
      rows = rows.filter(t => (t.message_count ?? 0) >= mp)
    }

    if (filters.pe_ratio) rows = rows.filter(t => {
      const pe = (t as any).pe_ratio ?? 0
      if (filters.pe_ratio === 'positive') return pe > 0
      if (filters.pe_ratio === 'low') return pe > 0 && pe < 15
      if (filters.pe_ratio === 'medium') return pe >= 15 && pe <= 25
      if (filters.pe_ratio === 'high') return pe > 25
      if (filters.pe_ratio === 'negative') return pe < 0
      return true
    })
    if (filters.forward_pe) rows = rows.filter(t => {
      const value = (t as any).forward_pe ?? 0
      if (filters.forward_pe === 'under10') return value < 10
      if (filters.forward_pe === 'under15') return value < 15
      if (filters.forward_pe === 'under25') return value < 25
      if (filters.forward_pe === 'over25') return value > 25
      return true
    })
    if (filters.peg) rows = rows.filter(t => {
      const value = (t as any).peg ?? 0
      if (filters.peg === 'under1') return value < 1
      if (filters.peg === 'under2') return value < 2
      if (filters.peg === 'over2') return value > 2
      return true
    })
    if (filters.dividend_yield) rows = rows.filter(t => {
      const value = (t as any).dividend_yield ?? 0
      if (filters.dividend_yield === 'positive') return value > 0
      if (filters.dividend_yield === 'over2') return value >= 2
      if (filters.dividend_yield === 'over4') return value >= 4
      return true
    })
    if (filters.analyst) rows = rows.filter(t => String((t as any).analyst || '') === filters.analyst)
    if (filters.rsi) rows = rows.filter(t => {
      const value = (t as any).rsi ?? 50
      if (filters.rsi === 'oversold') return value < 30
      if (filters.rsi === 'overbought') return value > 70
      if (filters.rsi === 'neutral') return value >= 30 && value <= 70
      return true
    })
    if (filters.sma20) rows = rows.filter(t => filters.sma20 === 'above' ? ((t as any).sma20 ?? 0) > 0 : ((t as any).sma20 ?? 0) < 0)
    for (const key of ['perf_week', 'perf_month', 'perf_year'] as const) {
      if (!filters[key]) continue
      rows = rows.filter(t => {
        const value = (t as any)[key] ?? 0
        if (filters[key] === 'up') return value > 0
        if (filters[key] === 'down') return value < 0
        if (filters[key] === 'up5') return value >= 5
        if (filters[key] === 'down5') return value <= -5
        if (filters[key] === 'up10') return value >= 10
        if (filters[key] === 'down10') return value <= -10
        if (filters[key] === 'up25') return value >= 25
        if (filters[key] === 'down25') return value <= -25
        return true
      })
    }
    if (filters.inst_own) rows = rows.filter(t => {
      const value = (t as any).inst_own ?? 0
      if (filters.inst_own === 'over50') return value >= 50
      if (filters.inst_own === 'over80') return value >= 80
      if (filters.inst_own === 'under30') return value < 30
      return true
    })
    if (filters.insider_own) rows = rows.filter(t => {
      const value = (t as any).insider_own ?? 0
      if (filters.insider_own === 'over5') return value >= 5
      if (filters.insider_own === 'over10') return value >= 10
      if (filters.insider_own === 'under1') return value < 1
      return true
    })
    if (filters.float_short) rows = rows.filter(t => {
      const value = (t as any).float_short ?? 0
      if (filters.float_short === 'over5') return value >= 5
      if (filters.float_short === 'over10') return value >= 10
      if (filters.float_short === 'over20') return value >= 20
      return true
    })

    // Signal
    if (signal === 'social_bullish') rows = rows.filter(t => (t.social_message_sentiment ?? t.social_sentiment ?? 0) >= 0.3)
    if (signal === 'social_bearish') rows = rows.filter(t => (t.social_message_sentiment ?? t.social_sentiment ?? 0) <= -0.3)
    if (signal === 'unusual_volume') rows = rows.filter(t => (t.volume ?? 0) > ((t as any).avg_volume ?? 1) * 2)
    if (signal === 'top_gainers') rows = rows.filter(t => (t.change_pct ?? 0) > 0)
    if (signal === 'top_losers') rows = rows.filter(t => (t.change_pct ?? 0) < 0)
    if (signal === 'bullish_news') rows = rows.filter(t => (t.structured_sentiment ?? 0) >= 0.2)
    if (signal === 'bearish_news') rows = rows.filter(t => (t.structured_sentiment ?? 0) <= -0.2)
    if (signal === 'oversold') rows = rows.filter(t => ((t as any).rsi ?? 50) < 30)
    if (signal === 'overbought') rows = rows.filter(t => ((t as any).rsi ?? 50) > 70)

    // Sort
    rows.sort((a, b) => {
      const av = (a as any)[orderBy] ?? 0
      const bv = (b as any)[orderBy] ?? 0
      if (typeof av === 'string') return orderDir === 'asc' ? av.localeCompare(bv) : bv.localeCompare(av)
      return orderDir === 'desc' ? bv - av : av - bv
    })

    return rows
  }, [tickers, filters, signal, orderBy, orderDir, search])

  const totalPages = Math.ceil(filtered.length / pageSize)
  const paged = filtered.slice(page * pageSize, (page + 1) * pageSize)
  const pricedCount = filtered.filter(t => t.price != null).length
  const gainers = filtered.filter(t => (t.change_pct ?? 0) > 0).length
  const losers = filtered.filter(t => (t.change_pct ?? 0) < 0).length
  const unchanged = filtered.filter(t => (t.change_pct ?? 0) === 0).length
  const highRelVol = filtered.filter(t => ((t as any).rel_volume ?? 0) >= 1.5).length
  const activeSocialRows = filtered.filter(t => Number(t.message_count ?? t.stocktwits_message_count ?? 0) > 0)
  const activeSocialCount = activeSocialRows.length
  const totalSocialMessages = filtered.reduce((sum, row) => sum + Number(row.message_count ?? row.stocktwits_message_count ?? 0), 0)
  const avgPostsPerActive = activeSocialCount ? totalSocialMessages / activeSocialCount : 0
  const avgSocialDensity = filtered.length
    ? filtered.reduce((sum, row) => {
      const windowMinutes = Math.max(1, Number((row as any).rolling_window_minutes ?? (socialWindow === 'adaptive' ? 30 : socialWindow) ?? 30))
      return sum + Number(row.message_count ?? row.stocktwits_message_count ?? 0) / windowMinutes
    }, 0) / filtered.length
    : 0
  const topMovers = [...filtered]
    .filter(row => Number(row.change_pct || 0) > 0)
    .sort((a, b) => Number(b.change_pct || 0) - Number(a.change_pct || 0))
    .slice(0, 4)
  const moverNews: Article[] = newsData?.articles ?? []
  const heatmap = useMemo(() => {
    const groups = new Map<string, { sector: string; count: number; avgChange: number; avgSentiment: number; totalMsgs: number; stocktwitsMsgs: number; activeSocial: number; totalDensity: number; stocktwitsDensity: number }>()
    for (const row of filtered) {
      const sector = row.sector || 'Unclassified'
      const current = groups.get(sector) || { sector, count: 0, avgChange: 0, avgSentiment: 0, totalMsgs: 0, stocktwitsMsgs: 0, activeSocial: 0, totalDensity: 0, stocktwitsDensity: 0 }
      const totalMessages = Number(row.message_count ?? row.stocktwits_message_count ?? 0)
      const stocktwitsMessages = Number(row.stocktwits_message_count ?? 0)
      const windowMinutes = Math.max(1, Number((row as any).rolling_window_minutes ?? (socialWindow === 'adaptive' ? 30 : socialWindow) ?? 30))
      const totalDensity = totalMessages / windowMinutes
      current.count += 1
      current.avgChange += Number(row.change_pct || 0)
      current.avgSentiment += Number(row.avg_sentiment || 0)
      current.totalMsgs += totalMessages
      current.stocktwitsMsgs += stocktwitsMessages
      current.totalDensity += totalDensity
      current.stocktwitsDensity += Number(row.social_message_density ?? 0)
      if (totalMessages > 0) current.activeSocial += 1
      groups.set(sector, current)
    }
    return Array.from(groups.values())
      .map(row => ({
        ...row,
        avgChange: row.count ? row.avgChange / row.count : 0,
        avgSentiment: row.count ? row.avgSentiment / row.count : 0,
        avgMsgsPerActive: row.activeSocial ? row.totalMsgs / row.activeSocial : 0,
        avgDensity: row.count ? row.totalDensity / row.count : 0,
        stocktwitsDensity: row.count ? row.stocktwitsDensity / row.count : 0,
      }))
      .sort((a, b) => (b.activeSocial - a.activeSocial) || (b.avgDensity - a.avgDensity) || (Math.abs(b.avgChange) - Math.abs(a.avgChange)))
      .slice(0, 12)
  }, [filtered, socialWindow])

  const setFilter = (k: string, v: string) => {
    setPage(0)
    if (v) setFilters(f => ({ ...f, [k]: v }))
    else setFilters(f => { const n = { ...f }; delete n[k]; return n })
  }

  const resetFilters = () => { setFilters({}); setSignal(''); setSearch(''); setPage(0) }

  const sectors = useMemo(() => [...new Set(tickers.map(t => t.sector).filter(Boolean))].sort() as string[], [tickers])
  const industries = useMemo(() => [...new Set(tickers.map(t => t.industry).filter(Boolean))].sort() as string[], [tickers])
  const countries = useMemo(() => [...new Set(tickers.map(t => (t as any).country).filter(Boolean))].sort() as string[], [tickers])

  const handleSort = (key: string) => {
    if (orderBy === key) {
      setOrderDir(d => d === 'asc' ? 'desc' : 'asc')
    } else {
      setOrderBy(key)
      setOrderDir(key === 'change_pct' || key === 'volume' ? 'desc' : 'asc')
    }
    setPage(0)
  }

  const selectCls = 'bg-bg border border-border rounded px-2 py-1 text-xs text-neutral hover:border-accent/50 focus:outline-none focus:border-accent transition-colors'

  return (
    <div>
      {/* Finviz-style compact toolbar */}
      <div className="flex items-center gap-1.5 flex-wrap mb-3 bg-surface border border-border rounded-lg px-3 py-2">
        <select
          value={signal}
          onChange={e => {
            const v = e.target.value
            setSignal(v)
            if (v === 'top_losers') { setOrderBy('change_pct'); setOrderDir('asc') }
            else if (v === 'unusual_volume') { setOrderBy('rel_volume'); setOrderDir('desc') }
            else if (v) { setOrderBy('change_pct'); setOrderDir('desc') }
            setPage(0)
          }}
          className={selectCls}
        >
          {PRESETS.map(p => <option key={p.key} value={p.key}>{p.label}</option>)}
        </select>

        <select value={(filters as any).exchange || ''} onChange={e => setFilter('exchange', e.target.value)} className={selectCls}>
          <option value="">Exchange</option>
          <option value="NASDAQ">NASDAQ</option>
          <option value="NYSE">NYSE</option>
          <option value="AMEX">AMEX</option>
        </select>

        <select value={filters.sector || ''} onChange={e => setFilter('sector', e.target.value)} className={selectCls}>
          <option value="">Sector</option>
          {sectors.map(s => <option key={s} value={s}>{s}</option>)}
        </select>

        <select value={filters.industry || ''} onChange={e => setFilter('industry', e.target.value)} className={selectCls}>
          <option value="">Industry</option>
          {industries.map(i => <option key={i} value={i}>{i}</option>)}
        </select>

        <select value={(filters as any).country || ''} onChange={e => setFilter('country', e.target.value)} className={selectCls}>
          <option value="">Country</option>
          {countries.map(c => <option key={c} value={c}>{c}</option>)}
        </select>

        <select value={filters.market_cap || ''} onChange={e => setFilter('market_cap', e.target.value)} className={selectCls}>
          {MARKET_CAP_OPTIONS.map(o => <option key={o.value} value={o.value}>{o.value ? `Market Cap: ${o.label}` : 'Market Cap'}</option>)}
        </select>

        <input
          value={search}
          onChange={e => { setSearch(e.target.value); setPage(0) }}
          placeholder="Ticker / Company…"
          className="bg-bg border border-border rounded px-2 py-1 text-xs text-neutral placeholder-slate-600 focus:outline-none focus:border-accent w-36"
        />

        <button
          onClick={() => setShowFilters(s => !s)}
          className={`text-xs px-2.5 py-1 rounded border transition-colors ${showFilters ? 'bg-accent/10 border-accent/40 text-accent' : 'border-border text-neutral hover:text-white hover:border-accent/50'}`}
        >
          More Filters {Object.keys(filters).filter(k => !['exchange','sector','industry','country','market_cap'].includes(k)).length > 0 ? `(${Object.keys(filters).filter(k => !['exchange','sector','industry','country','market_cap'].includes(k)).length})` : ''}
        </button>

        {(Object.keys(filters).length > 0 || signal || search) && (
          <button onClick={resetFilters} className="text-xs text-red-400 hover:text-red-300 px-1">Reset</button>
        )}

        <button onClick={() => mutate()} className="text-xs px-2.5 py-1 rounded border border-border text-neutral hover:text-white hover:border-accent/50 transition-colors">
          ↻ Refresh
        </button>

        <div className="ml-auto flex items-center gap-2">
          <span className="text-[10px] text-neutral uppercase">Social</span>
          <select
            value={socialWindow}
            onChange={event => setSocialWindow(event.target.value)}
            className={selectCls}
          >
            <option value="adaptive">Adaptive</option>
            <option value="5">5m</option>
            <option value="15">15m</option>
            <option value="30">30m</option>
            <option value="60">1h</option>
            <option value="120">2h</option>
            <option value="1440">24h</option>
          </select>
          <span className="text-neutral text-xs whitespace-nowrap">{filtered.length} stocks</span>
        </div>
      </div>

      {/* Active filter pills */}
      {Object.keys(filters).length > 0 && (
        <div className="flex items-center gap-1.5 flex-wrap mb-2">
          {Object.entries(filters).map(([k, v]) => (
            <span key={k} className="flex items-center gap-1 text-[11px] bg-accent/10 border border-accent/30 text-accent px-2 py-0.5 rounded">
              {k}: {v}
              <button onClick={() => setFilter(k, '')} className="hover:text-white ml-0.5 leading-none">&times;</button>
            </span>
          ))}
        </div>
      )}

      {/* More filters panel */}
      {showFilters && (
        <ScreenerFilterPanel
          filters={filters}
          setFilter={setFilter}
          activeTab={filterTab}
          setActiveTab={setFilterTab}
        />
      )}

      <div className="flex items-center justify-between gap-3 mb-3">
        <h1 className="text-white font-semibold text-lg">Market Screener</h1>
      </div>

      <div className="grid grid-cols-2 md:grid-cols-5 gap-2 mb-3">
        <ScreenerMetric label="Universe" value={compact(filtered.length)} />
        <ScreenerMetric label="Priced" value={compact(pricedCount)} tone="text-sky-300" />
        <ScreenerMetric label="Breadth G/L/F" value={`${gainers}/${losers}/${unchanged}`} tone={losers ? 'text-emerald-300' : 'text-yellow-300'} />
        <ScreenerMetric label="Active Social" value={`${compact(activeSocialCount)}/${compact(filtered.length)}`} tone="text-indigo-300" />
        <ScreenerMetric label="Avg Posts" value={`${compact(avgPostsPerActive)} active`} tone="text-violet-300" subvalue={`${avgSocialDensity.toFixed(2)}/m avg density`} />
      </div>

      <div className="grid grid-cols-1 xl:grid-cols-[minmax(0,1.25fr)_minmax(360px,0.9fr)] gap-3 mb-3">
        <section className="bg-surface border border-border rounded-lg overflow-hidden">
          <div className="px-3 py-2 border-b border-border flex items-center justify-between">
            <span className="text-xs uppercase text-neutral font-medium">Sector Heatmap</span>
            <span className="text-[10px] text-neutral">filtered universe</span>
          </div>
          <div className="p-2 grid grid-cols-2 md:grid-cols-3 lg:grid-cols-4 gap-2">
            {heatmap.map(tile => (
              <div
                key={tile.sector}
                className={`rounded border px-2 py-2 min-h-[70px] ${
                  tile.avgChange >= 0
                    ? 'bg-emerald-500/10 border-emerald-500/25'
                    : 'bg-red-500/10 border-red-500/25'
                }`}
              >
                <div className="text-xs text-white truncate">{tile.sector}</div>
                <div className={`font-mono text-lg ${tile.avgChange >= 0 ? 'text-emerald-400' : 'text-red-400'}`}>
                  {tile.avgChange >= 0 ? '+' : ''}{tile.avgChange.toFixed(1)}%
                </div>
                <div className="text-[10px] text-neutral">{tile.activeSocial}/{tile.count} active social</div>
                <div className="text-[10px] text-neutral">{compact(tile.avgMsgsPerActive)} avg posts · {tile.avgDensity.toFixed(2)}/m</div>
              </div>
            ))}
          </div>
        </section>

        <section className="bg-surface border border-border rounded-lg overflow-hidden">
          <div className="px-3 py-2 border-b border-border flex items-center justify-between">
            <span className="text-xs uppercase text-neutral font-medium">Mover News</span>
            <span className="text-[10px] text-neutral">{moverNews.length} latest</span>
          </div>
          <div className="divide-y divide-slate-700/30 max-h-[230px] overflow-y-auto">
            {moverNews.length ? moverNews.map(article => (
              <a key={article.id || article.article_id || article.url} href={article.url || '#'} target="_blank" rel="noreferrer" className="block px-3 py-2 hover:bg-bg/50">
                <div className="flex items-center gap-2 text-[10px] mb-1">
                  <span className="font-mono text-accent">{article.matched_mover_tickers?.join(',') || article.ticker || '--'}</span>
                  <span className="text-neutral truncate">{article.source}</span>
                </div>
                <div className="text-xs text-slate-200 line-clamp-2">{article.title}</div>
              </a>
            )) : (
              <div className="px-3 py-8 text-sm text-neutral text-center">No mover-matched news in the current window.</div>
            )}
          </div>
        </section>
      </div>

      <div className="grid grid-cols-1 md:grid-cols-2 xl:grid-cols-4 gap-3 mb-3">
        {topMovers.map(row => (
          <div key={row.ticker} className="bg-surface border border-border rounded-lg overflow-hidden">
            <div className="px-3 py-2 border-b border-border flex items-center justify-between">
              <div>
                <div className="font-mono text-accent font-semibold">{row.ticker}</div>
                <div className="text-[10px] text-neutral truncate max-w-[150px]">{row.company || row.sector}</div>
              </div>
              <div className="text-right">
                <div className="font-mono text-emerald-400">+{Number(row.change_pct || 0).toFixed(1)}%</div>
                <div className="text-[10px] text-neutral">{row.rolling_window_minutes ?? '--'}m window</div>
              </div>
            </div>
            <div className="h-[130px]">
              <IntradayChart ticker={row.ticker} />
            </div>
          </div>
        ))}
      </div>

      {/* View mode tabs */}
      <div className="flex items-center gap-1 mb-3 border-b border-border">
        {VIEW_MODES.map(mode => (
          <button
            key={mode}
            onClick={() => setViewMode(mode)}
            className={`px-3 py-1.5 text-xs capitalize transition-colors border-b-2 -mb-px ${
              viewMode === mode
                ? 'text-white border-accent'
                : 'text-neutral border-transparent hover:text-white'
            }`}
          >
            {mode}
          </button>
        ))}
      </div>

      {/* Table */}
      <ScreenerTable
        rows={paged}
        isLoading={isLoading}
        viewMode={viewMode}
        pageOffset={page * pageSize}
        onSort={handleSort}
        sortKey={orderBy}
        sortDir={orderDir}
      />

      {/* Pagination */}
      {totalPages > 1 && (
        <div className="flex items-center justify-between mt-3">
          <span className="text-xs text-neutral">
            {page * pageSize + 1}–{Math.min((page + 1) * pageSize, filtered.length)} of {filtered.length}
          </span>
          <div className="flex gap-1">
            <button disabled={page === 0} onClick={() => setPage(p => p - 1)}
              className="px-2 py-1 text-xs bg-surface border border-border rounded text-neutral disabled:opacity-40 hover:text-white">Prev</button>
            {Array.from({ length: Math.min(5, totalPages) }, (_, i) => {
              let pn: number
              if (totalPages <= 5) pn = i
              else if (page < 3) pn = i
              else if (page >= totalPages - 3) pn = totalPages - 5 + i
              else pn = page - 2 + i
              return (
                <button key={pn} onClick={() => setPage(pn)}
                  className={`w-6 h-6 text-xs rounded ${page === pn ? 'bg-accent text-white' : 'bg-surface border border-border text-neutral hover:text-white'}`}>
                  {pn + 1}
                </button>
              )
            })}
            <button disabled={page >= totalPages - 1} onClick={() => setPage(p => p + 1)}
              className="px-2 py-1 text-xs bg-surface border border-border rounded text-neutral disabled:opacity-40 hover:text-white">Next</button>
          </div>
        </div>
      )}
    </div>
  )
}

function ScreenerMetric({ label, value, tone = 'text-white', subvalue }: { label: string; value: string; tone?: string; subvalue?: string }) {
  return (
    <div className="bg-surface border border-border rounded-lg px-3 py-2 min-w-0">
      <div className={`font-mono text-lg font-semibold truncate ${tone}`}>{value}</div>
      <div className="text-[10px] uppercase text-neutral mt-0.5">{label}</div>
      {subvalue && <div className="text-[10px] text-slate-500 mt-0.5">{subvalue}</div>}
    </div>
  )
}
