'use client'
import { useState } from 'react'
import { ScreenerRow } from './ScreenerRow'
import type { ScreenerRow as SR } from '@/lib/types'
import type { ViewMode } from './ScreenerPage'

interface Props {
  rows: SR[]
  isLoading: boolean
  viewMode: ViewMode
  pageOffset?: number
  onSort?: (key: string) => void
  sortKey?: string
  sortDir?: 'asc' | 'desc'
}

const COLUMNS: Record<ViewMode, Array<{ key: string; label: string; sortable?: boolean }>> = {
  overview: [
    { key: 'no', label: 'No.' },
    { key: 'ticker', label: 'Ticker', sortable: true },
    { key: 'company', label: 'Company' },
    { key: 'sector', label: 'Sector', sortable: true },
    { key: 'industry', label: 'Industry', sortable: true },
    { key: 'country', label: 'Country', sortable: true },
    { key: 'market_cap', label: 'Market Cap', sortable: true },
    { key: 'pe_ratio', label: 'P/E', sortable: true },
    { key: 'price', label: 'Price', sortable: true },
    { key: 'change_pct', label: 'Change', sortable: true },
    { key: 'volume', label: 'Volume', sortable: true },
  ],
  performance: [
    { key: 'no', label: 'No.' },
    { key: 'ticker', label: 'Ticker', sortable: true },
    { key: 'change_pct', label: 'Change', sortable: true },
    { key: 'perf_week', label: 'Week', sortable: true },
    { key: 'perf_month', label: 'Month', sortable: true },
    { key: 'perf_quarter', label: 'Quarter', sortable: true },
    { key: 'perf_half', label: 'Half Y', sortable: true },
    { key: 'perf_year', label: 'Year', sortable: true },
    { key: 'perf_ytd', label: 'YTD', sortable: true },
  ],
  technical: [
    { key: 'no', label: 'No.' },
    { key: 'ticker', label: 'Ticker', sortable: true },
    { key: 'price', label: 'Price', sortable: true },
    { key: 'change_pct', label: 'Change', sortable: true },
    { key: 'volume', label: 'Volume', sortable: true },
    { key: 'avg_volume', label: 'Avg Vol', sortable: true },
    { key: 'rel_volume', label: 'Rel Vol', sortable: true },
    { key: 'rsi', label: 'RSI', sortable: true },
    { key: 'sma20', label: 'SMA20', sortable: true },
    { key: 'sma50', label: 'SMA50', sortable: true },
    { key: 'sma200', label: 'SMA200', sortable: true },
    { key: 'atr', label: 'ATR', sortable: true },
    { key: 'gap', label: 'Gap', sortable: true },
  ],
  sentiment: [
    { key: 'no', label: 'No.' },
    { key: 'ticker', label: 'Ticker', sortable: true },
    { key: 'social_message_sentiment', label: 'ST Sent', sortable: true },
    { key: 'social_message_density', label: 'ST Dens', sortable: true },
    { key: 'stocktwits_message_count', label: 'ST Msgs', sortable: true },
    { key: 'social_sentiment', label: 'All Social', sortable: true },
    { key: 'message_count', label: 'All Posts', sortable: true },
    { key: 'rolling_window_minutes', label: 'Window', sortable: true },
    { key: 'structured_sentiment', label: 'News', sortable: true },
    { key: 'news_article_count', label: 'Articles', sortable: true },
    { key: 'bullish_count', label: 'Bull', sortable: true },
    { key: 'bearish_count', label: 'Bear', sortable: true },
  ],
}

export function ScreenerTable({ rows, isLoading, viewMode, pageOffset = 0, onSort, sortKey, sortDir }: Props) {
  const [expandedTicker, setExpandedTicker] = useState<string | null>(null)
  const columns = COLUMNS[viewMode]

  const handleExpand = (ticker: string) => {
    setExpandedTicker(t => t === ticker ? null : ticker)
  }

  if (isLoading) {
    return <div className="text-neutral text-sm animate-pulse p-4">Loading screener data…</div>
  }
  if (rows.length === 0) {
    return (
      <div className="text-center py-12 text-neutral">
        <div className="text-2xl mb-2">🔍</div>
        <div className="text-sm">No tickers match current filters</div>
      </div>
    )
  }

  return (
    <div className="bg-surface border border-border rounded-lg overflow-hidden">
      <div className="overflow-x-auto">
        <table className="w-full text-xs">
          <thead>
            <tr className="bg-[#0d1b2a] border-b border-border">
              {columns.map(col => (
                <th
                  key={col.key}
                  className={`px-2 py-2 text-left text-[10px] text-neutral uppercase tracking-wide font-medium whitespace-nowrap ${
                    col.sortable && onSort ? 'cursor-pointer hover:text-white select-none' : ''
                  } ${sortKey === col.key ? 'text-accent' : ''}`}
                  onClick={() => col.sortable && onSort && onSort(col.key)}
                >
                  {col.label}
                  {sortKey === col.key && (
                    <span className="ml-0.5 text-accent">{sortDir === 'asc' ? '▲' : '▼'}</span>
                  )}
                </th>
              ))}
            </tr>
          </thead>
          <tbody className="divide-y divide-slate-700/20">
            {rows.map((row, i) => (
              <ScreenerRow
                key={row.ticker}
                row={row}
                columns={columns}
                rowIndex={pageOffset + i + 1}
                colSpan={columns.length}
                expanded={expandedTicker === row.ticker}
                onExpand={() => handleExpand(row.ticker)}
              />
            ))}
          </tbody>
        </table>
      </div>
    </div>
  )
}
