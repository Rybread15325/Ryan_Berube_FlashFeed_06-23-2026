'use client'
import { useState } from 'react'
import useSWR from 'swr'
import type { Article, ScreenerRow as SR } from '@/lib/types'
import { CandlestickChart } from './CandlestickChart'

const fetcher = (url: string) => fetch(url).then(r => r.json())

interface Props {
  ticker: string
  row: SR
  colSpan: number
  onClose: () => void
}

function fmt(n: number | null | undefined, digits = 2): string {
  if (n == null || Number.isNaN(n)) return '—'
  return Number(n).toFixed(digits)
}

function fmtPct(n: number | null | undefined): string {
  if (n == null || Number.isNaN(n)) return '—'
  const v = Number(n)
  return `${v > 0 ? '+' : ''}${v.toFixed(2)}%`
}

function fmtM(n: number | null | undefined): string {
  if (n == null) return '—'
  const v = Number(n)
  if (v >= 1e12) return `${(v / 1e12).toFixed(1)}T`
  if (v >= 1e9) return `${(v / 1e9).toFixed(1)}B`
  if (v >= 1e6) return `${(v / 1e6).toFixed(1)}M`
  if (v >= 1e3) return `${(v / 1e3).toFixed(0)}K`
  return v.toFixed(0)
}

function analystColor(a: string | null | undefined) {
  if (!a) return 'text-neutral'
  if (a === 'Buy' || a === 'Strong Buy') return 'text-emerald-400'
  if (a === 'Sell' || a === 'Strong Sell') return 'text-red-400'
  return 'text-yellow-300'
}

export function TickerMirror({ ticker, row, colSpan, onClose }: Props) {
  const [grokText, setGrokText] = useState<string | null>(null)
  const [grokLoading, setGrokLoading] = useState(false)

  const { data: chartData } = useSWR(`/api/charts/${ticker}?range=5d&interval=30m`, fetcher)
  const { data: newsData } = useSWR(`/api/articles?ticker=${ticker}&limit=5&recent_days=30`, fetcher)

  const candles = chartData?.candles ?? []
  const bollinger = chartData?.bollinger
  const predicted = chartData?.predicted ?? []
  const news: Article[] = newsData?.articles ?? []

  const w52range =
    row.low_52w != null && row.high_52w != null
      ? `$${row.low_52w.toFixed(2)} – $${row.high_52w.toFixed(2)}`
      : '—'

  const fundamentals: [string, string][] = [
    ['Market Cap', fmtM(row.market_cap)],
    ['P/E', fmt(row.pe_ratio, 1)],
    ['Fwd P/E', fmt(row.forward_pe, 1)],
    ['PEG', fmt(row.peg, 2)],
    ['P/S', fmt(row.ps_ratio, 2)],
    ['P/B', fmt(row.pb_ratio, 2)],
    ['Dividend', row.dividend_yield != null ? `${row.dividend_yield.toFixed(2)}%` : '—'],
    ['Insider Own', row.insider_own != null ? `${row.insider_own.toFixed(1)}%` : '—'],
    ['Short Float', row.float_short != null ? `${row.float_short.toFixed(1)}%` : '—'],
    ['Analyst', row.analyst ?? '—'],
    ['Avg Volume', fmtM(row.avg_volume)],
    ['EPS Next Y', fmtPct(row.eps_growth_next_y)],
    ['Sales Q/Q', fmtPct(row.sales_growth)],
    ['Inst Own', row.inst_own != null ? `${row.inst_own.toFixed(1)}%` : '—'],
    ['Target Price', row.target_price != null ? `$${row.target_price.toFixed(2)}` : '—'],
    ['52W Range', w52range],
    ['Beta', fmt(row.beta, 2)],
    ['Earnings', row.earnings_date ?? '—'],
  ]

  const runGrok = async () => {
    setGrokLoading(true)
    setGrokText(null)
    try {
      const resp = await fetch('/api/grok/analyze', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          ticker,
          context: [
            `Price: $${row.price?.toFixed(2) ?? '?'}`,
            `Change: ${row.change_pct?.toFixed(2) ?? '?'}%`,
            `Sector: ${row.sector ?? '?'}`,
            `P/E: ${row.pe_ratio?.toFixed(1) ?? '?'}`,
            `RSI: ${(row.rsi ?? 0).toFixed(1)}`,
            `News Sentiment: ${(row.structured_sentiment ?? 0).toFixed(2)}`,
            `Analyst: ${row.analyst ?? '?'}`,
          ].join(', '),
        }),
      })
      const json = await resp.json()
      setGrokText(json.analysis ?? json.error ?? 'No response')
    } catch (e: any) {
      setGrokText(`Error: ${e.message}`)
    } finally {
      setGrokLoading(false)
    }
  }

  return (
    <tr>
      <td colSpan={colSpan} className="p-0 border-b border-border">
        <div className="bg-[#080f1a]">
          {/* Mirror header */}
          <div className="flex items-center justify-between px-4 py-2 border-b border-border/50 bg-[#0c1420]">
            <div className="flex items-center gap-3">
              <span className="font-mono font-bold text-accent text-sm">{ticker}</span>
              {row.price != null && (
                <span className="font-mono text-white">${row.price.toFixed(2)}</span>
              )}
              {row.change_pct != null && (
                <span className={`font-mono text-sm ${row.change_pct >= 0 ? 'text-emerald-400' : 'text-red-400'}`}>
                  {row.change_pct >= 0 ? '+' : ''}{row.change_pct.toFixed(2)}%
                </span>
              )}
              {row.company && <span className="text-neutral text-xs">{row.company}</span>}
              {row.sector && <span className="text-[10px] text-slate-500 hidden sm:inline">{row.sector}</span>}
            </div>
            <div className="flex items-center gap-2">
              <button
                onClick={runGrok}
                disabled={grokLoading}
                className="text-[11px] px-2 py-1 bg-violet-600/20 border border-violet-500/30 text-violet-300 rounded hover:bg-violet-600/30 disabled:opacity-50 transition-colors"
              >
                {grokLoading ? 'Analyzing…' : '✦ Grok Analysis'}
              </button>
              <button
                onClick={onClose}
                className="text-neutral hover:text-white text-xl leading-none w-6 h-6 flex items-center justify-center"
              >
                ×
              </button>
            </div>
          </div>

          {/* Grok result */}
          {grokText && (
            <div className="mx-3 mt-2 p-3 bg-violet-900/20 border border-violet-500/20 rounded text-xs text-slate-200">
              <span className="text-violet-400 font-semibold mr-1">✦ Grok:</span>
              {grokText}
            </div>
          )}

          {/* Chart + Fundamentals */}
          <div className="flex gap-0 divide-x divide-border/20">
            <div className="flex-1 min-w-0 p-3">
              <div className="h-[280px]">
                {candles.length > 0
                  ? (
                    <CandlestickChart
                      candles={candles}
                      bollinger={bollinger}
                      predicted={predicted}
                      newsEvents={[]}
                    />
                  )
                  : (
                    <div className="h-full flex items-center justify-center text-neutral text-xs animate-pulse">
                      Loading chart…
                    </div>
                  )
                }
              </div>
            </div>

            <div className="w-[260px] shrink-0 p-3">
              <table className="w-full text-[11px]">
                <tbody>
                  {fundamentals.map(([label, value]) => (
                    <tr key={label} className="border-b border-border/15">
                      <td className="py-[3px] text-neutral pr-2">{label}</td>
                      <td className={`py-[3px] font-mono text-right ${
                        label === 'Analyst' ? analystColor(row.analyst) : 'text-white'
                      }`}>{value}</td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          </div>

          {/* News */}
          <div className="border-t border-border/20 px-4 py-3">
            <div className="text-[10px] uppercase text-neutral mb-2 font-medium tracking-wide">Recent News</div>
            {news.length === 0 ? (
              <div className="text-xs text-neutral">No recent news found for {ticker}.</div>
            ) : (
              <div className="flex flex-col gap-2">
                {news.map(a => (
                  <a
                    key={a.id || a.article_id || a.url}
                    href={a.url || '#'}
                    target="_blank"
                    rel="noreferrer"
                    className="flex items-start gap-2 group"
                  >
                    <span className={`shrink-0 mt-0.5 text-[9px] font-semibold uppercase px-1.5 py-0.5 rounded ${
                      a.sentiment === 'bullish'
                        ? 'bg-emerald-500/15 text-emerald-400'
                        : a.sentiment === 'bearish'
                        ? 'bg-red-500/15 text-red-400'
                        : 'bg-slate-600/20 text-slate-400'
                    }`}>
                      {a.sentiment ?? 'N'}
                    </span>
                    <div className="min-w-0">
                      <div className="text-xs text-slate-200 group-hover:text-white leading-snug line-clamp-1">
                        {a.title}
                      </div>
                      <div className="text-[10px] text-neutral mt-0.5">
                        {a.source}
                        {a.publish_date
                          ? ` · ${new Date(a.publish_date * 1000).toLocaleDateString()}`
                          : ''}
                      </div>
                    </div>
                  </a>
                ))}
              </div>
            )}
          </div>
        </div>
      </td>
    </tr>
  )
}
