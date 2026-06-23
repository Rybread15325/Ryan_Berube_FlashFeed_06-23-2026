'use client'
import { clsx } from 'clsx'
import type { ScreenerRow as SR } from '@/lib/types'
import { TickerMirror } from './TickerMirror'

interface Props {
  row: SR
  columns: Array<{ key: string; label: string }>
  rowIndex: number
  colSpan: number
  expanded: boolean
  onExpand: () => void
}

function fmtCompact(n: number | undefined | null): string {
  if (n == null) return '—'
  if (n >= 1e12) return `${(n / 1e12).toFixed(1)}T`
  if (n >= 1e9) return `${(n / 1e9).toFixed(1)}B`
  if (n >= 1e6) return `${(n / 1e6).toFixed(1)}M`
  if (n >= 1e3) return `${(n / 1e3).toFixed(0)}K`
  return n.toLocaleString()
}

function fmtNumber(n: number | undefined | null, digits = 2): string {
  if (n == null || Number.isNaN(n)) return '—'
  return Number(n).toFixed(digits)
}

function fmtPct(n: number | undefined | null, signed = false): string {
  if (n == null || Number.isNaN(n)) return '—'
  const sign = signed && n > 0 ? '+' : ''
  return `${sign}${Number(n).toFixed(1)}%`
}

function pctTone(n: number | undefined | null) {
  const value = Number(n ?? 0)
  return value > 0 ? 'text-emerald-400' : value < 0 ? 'text-red-400' : 'text-neutral'
}

function sentBar(bullish: number, bearish: number, neutral: number) {
  const total = bullish + bearish + neutral
  if (total === 0) return null
  const bp = (bullish / total) * 100
  const np = (neutral / total) * 100
  return (
    <div className="flex h-1.5 rounded-full overflow-hidden w-16">
      <div className="bg-emerald-500" style={{ width: `${bp}%` }} />
      <div className="bg-slate-500" style={{ width: `${np}%` }} />
      <div className="bg-red-500" style={{ width: `${100 - bp - np}%` }} />
    </div>
  )
}

export function ScreenerRow({ row, columns, rowIndex, colSpan, expanded, onExpand }: Props) {
  const renderCell = (key: string) => {
    switch (key) {
      case 'no':
        return <span className="text-neutral font-mono">{rowIndex}</span>
      case 'ticker':
        return (
          <button
            onClick={onExpand}
            className={clsx(
              'font-mono font-bold transition-colors',
              expanded ? 'text-sky-300' : 'text-accent hover:text-sky-300'
            )}
          >
            {row.ticker}
          </button>
        )
      case 'company':
        return <span className="text-slate-300 truncate block max-w-[150px]">{row.company || row.industry || '—'}</span>
      case 'exchange':
      case 'country':
      case 'index':
      case 'market_cap_bucket':
      case 'earnings_date':
        return <span className="text-neutral whitespace-nowrap">{(row as any)[key] ?? '—'}</span>
      case 'price':
        return <span className="font-mono">{row.price != null ? `$${row.price.toFixed(2)}` : '—'}</span>
      case 'change_pct':
        return (
          <span className={clsx('font-mono', (row.change_pct ?? 0) >= 0 ? 'text-emerald-400' : 'text-red-400')}>
            {row.change_pct != null ? `${row.change_pct >= 0 ? '+' : ''}${row.change_pct.toFixed(2)}%` : '—'}
          </span>
        )
      case 'volume':
        return <span className="font-mono text-neutral">{fmtCompact(row.volume)}</span>
      case 'avg_volume':
        return <span className="font-mono text-neutral">{fmtCompact((row as any).avg_volume)}</span>
      case 'rel_volume':
        return <span className="font-mono text-neutral">{fmtNumber((row as any).rel_volume, 2)}x</span>
      case 'market_cap':
        return <span className="font-mono text-neutral">{fmtCompact((row as any).market_cap)}</span>
      case 'pe_ratio':
      case 'forward_pe':
      case 'peg':
      case 'ps_ratio':
      case 'pb_ratio':
      case 'debt_equity':
      case 'beta':
      case 'atr':
        return <span className="font-mono text-neutral">{fmtNumber((row as any)[key], key === 'pe_ratio' || key === 'forward_pe' ? 1 : 2)}</span>
      case 'sector':
        return <span className="text-neutral truncate block max-w-[120px]">{row.sector ?? '—'}</span>
      case 'industry':
        return <span className="text-neutral truncate block max-w-[120px]">{row.industry ?? '—'}</span>
      case 'avg_sentiment':
        const avgSentiment = row.avg_sentiment ?? 0
        return (
          <div className="flex items-center gap-1.5">
            <span className={clsx('font-mono', avgSentiment >= 0.2 ? 'text-emerald-400' : avgSentiment <= -0.2 ? 'text-red-400' : 'text-neutral')}>
              {avgSentiment.toFixed(2)}
            </span>
            {sentBar(row.bullish_count ?? 0, row.bearish_count ?? 0, row.neutral_count ?? 0)}
          </div>
        )
      case 'social_sentiment':
        const socialSentiment = row.social_sentiment ?? 0
        return (
          <span className={clsx('font-mono', socialSentiment >= 0.2 ? 'text-emerald-400' : socialSentiment <= -0.2 ? 'text-red-400' : 'text-neutral')}>
            {socialSentiment.toFixed(2)}
          </span>
        )
      case 'social_message_sentiment':
        const stocktwitsSentiment = row.social_message_sentiment ?? 0
        return (
          <span className={clsx('font-mono', stocktwitsSentiment >= 0.2 ? 'text-emerald-400' : stocktwitsSentiment <= -0.2 ? 'text-red-400' : 'text-neutral')}>
            {stocktwitsSentiment.toFixed(2)}
          </span>
        )
      case 'social_message_density':
        return <span className="font-mono text-neutral">{(row.social_message_density ?? 0).toFixed(3)}/m</span>
      case 'stocktwits_message_count':
        return <span className="font-mono text-neutral">{row.stocktwits_message_count ?? 0}</span>
      case 'structured_sentiment':
        const structuredSentiment = row.structured_sentiment ?? 0
        return (
          <span className={clsx('font-mono', structuredSentiment >= 0.2 ? 'text-emerald-400' : structuredSentiment <= -0.2 ? 'text-red-400' : 'text-neutral')}>
            {structuredSentiment.toFixed(2)}
          </span>
        )
      case 'message_count':
        return <span className="font-mono text-neutral">{row.message_count ?? 0}</span>
      case 'rolling_window_minutes':
        return <span className="font-mono text-neutral">{(row as any).rolling_window_minutes ?? '—'}m</span>
      case 'news_article_count':
        return <span className="font-mono text-neutral">{row.news_article_count ?? 0}</span>
      case 'bullish_count':
        return <span className="font-mono text-emerald-400">{row.bullish_count ?? 0}</span>
      case 'bearish_count':
        return <span className="font-mono text-red-400">{row.bearish_count ?? 0}</span>
      case 'target_price':
        return <span className="font-mono text-neutral">{(row as any).target_price != null ? `$${Number((row as any).target_price).toFixed(2)}` : '—'}</span>
      case 'dividend_yield':
      case 'eps_growth_this_y':
      case 'eps_growth_next_y':
      case 'sales_growth':
      case 'gross_margin':
      case 'operating_margin':
      case 'roe':
      case 'inst_own':
      case 'insider_own':
      case 'float_short':
      case 'perf_week':
      case 'perf_month':
      case 'perf_quarter':
      case 'perf_half':
      case 'perf_year':
      case 'perf_ytd':
      case 'sma20':
      case 'sma50':
      case 'sma200':
      case 'gap':
        return <span className={`font-mono ${pctTone((row as any)[key])}`}>{fmtPct((row as any)[key], ['perf_week','perf_month','perf_quarter','perf_half','perf_year','perf_ytd','sma20','sma50','sma200','gap','eps_growth_this_y','eps_growth_next_y','sales_growth'].includes(key))}</span>
      case 'rsi':
        const rsi = Number((row as any).rsi ?? 0)
        return <span className={clsx('font-mono', rsi >= 70 ? 'text-red-400' : rsi <= 30 ? 'text-emerald-400' : 'text-neutral')}>{fmtNumber(rsi, 1)}</span>
      case 'analyst':
        return <span className={clsx('font-mono', row.analyst === 'Buy' || row.analyst === 'Strong Buy' ? 'text-emerald-400' : row.analyst === 'Sell' ? 'text-red-400' : 'text-neutral')}>{row.analyst ?? '—'}</span>
      case 'sources':
        return (
          <div className="flex gap-0.5 flex-wrap">
            {(row.sources ?? []).slice(0, 3).map(s => (
              <span key={s} className="text-[9px] bg-slate-700 text-neutral px-1 py-0.5 rounded capitalize">{s}</span>
            ))}
          </div>
        )
      default:
        return <span className="text-neutral">—</span>
    }
  }

  return (
    <>
      <tr
        className={clsx(
          'hover:bg-card-hover transition-colors cursor-pointer',
          expanded && 'bg-card-hover'
        )}
        onClick={e => {
          if ((e.target as HTMLElement).tagName === 'BUTTON') return
          onExpand()
        }}
      >
        {columns.map(col => (
          <td key={col.key} className="px-2 py-1.5 whitespace-nowrap">{renderCell(col.key)}</td>
        ))}
      </tr>
      {expanded && (
        <TickerMirror
          ticker={row.ticker}
          row={row}
          colSpan={colSpan}
          onClose={onExpand}
        />
      )}
    </>
  )
}
