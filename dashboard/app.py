import os
import psycopg2
import pandas as pd
import plotly.graph_objects as go
import dash_bootstrap_components as dbc
from dash import Dash, dcc, html, Input, Output
from dotenv import load_dotenv

load_dotenv()

DB_CONFIG = {
    "dbname": os.getenv("POSTGRES_DB"),
    "user": os.getenv("POSTGRES_USER"),
    "password": os.getenv("POSTGRES_PASSWORD"),
    "host": os.getenv("POSTGRES_HOST"),
    "port": os.getenv("POSTGRES_PORT"),
}

app = Dash(__name__, external_stylesheets=[dbc.themes.CYBORG])
app.title = "Crypto Pipeline Dashboard"


def get_latest_prices():
    conn = psycopg2.connect(**DB_CONFIG)
    df = pd.read_sql("""
        SELECT DISTINCT ON (coin_id)
            c.name,
            c.symbol,
            p.price_usd,
            p.market_cap,
            p.volume_24h,
            p.price_change_24h,
            p.fetched_at
        FROM prices p
        JOIN coins c ON c.id = p.coin_id
        ORDER BY coin_id, fetched_at DESC
    """, conn)
    conn.close()
    return df


def get_price_history(coin_id="bitcoin"):
    conn = psycopg2.connect(**DB_CONFIG)
    df = pd.read_sql("""
        SELECT p.price_usd, p.fetched_at
        FROM prices p
        JOIN coins c ON c.id = p.coin_id
        WHERE c.id = %s
        ORDER BY p.fetched_at ASC
    """, conn, params=(coin_id,))
    conn.close()
    return df


app.layout = dbc.Container([
    dbc.Row([
        dbc.Col([
            html.H1("Crypto Pipeline", className="text-center my-4 fw-bold"),
            html.P("Live data ingested from CoinGecko API", className="text-center text-muted mb-4"),
        ])
    ]),

    # KPI Cards
    dbc.Row(id="kpi-cards", className="mb-4"),

    # Price History Chart
    dbc.Row([
        dbc.Col([
            dbc.Card([
                dbc.CardHeader([
                    dbc.Row([
                        dbc.Col(html.H5("Price History", className="mb-0")),
                        dbc.Col(
                            dcc.Dropdown(
                                id="coin-selector",
                                options=[
                                    {"label": "Bitcoin", "value": "bitcoin"},
                                    {"label": "Ethereum", "value": "ethereum"},
                                    {"label": "Solana", "value": "solana"},
                                    {"label": "Cardano", "value": "cardano"},
                                    {"label": "Polkadot", "value": "polkadot"},
                                ],
                                value="bitcoin",
                                clearable=False,
                                style={"color": "#000"}
                            ),
                            width=3
                        )
                    ])
                ]),
                dbc.CardBody([
                    dcc.Graph(id="price-history-chart")
                ])
            ], className="mb-4")
        ])
    ]),

    # Market Cap & Volume
    dbc.Row([
        dbc.Col([
            dbc.Card([
                dbc.CardHeader(html.H5("Market Cap", className="mb-0")),
                dbc.CardBody([dcc.Graph(id="market-cap-chart")])
            ])
        ], width=6),
        dbc.Col([
            dbc.Card([
                dbc.CardHeader(html.H5("24h Volume", className="mb-0")),
                dbc.CardBody([dcc.Graph(id="volume-chart")])
            ])
        ], width=6),
    ], className="mb-4"),

    dcc.Interval(id="interval", interval=30 * 1000, n_intervals=0),

], fluid=True)


@app.callback(
    Output("kpi-cards", "children"),
    Output("market-cap-chart", "figure"),
    Output("volume-chart", "figure"),
    Input("interval", "n_intervals")
)
def update_overview(n):
    df = get_latest_prices()

    # KPI Cards
    cards = []
    for _, row in df.iterrows():
        color = "success" if row["price_change_24h"] >= 0 else "danger"
        arrow = "▲" if row["price_change_24h"] >= 0 else "▼"
        cards.append(
            dbc.Col(
                dbc.Card([
                    dbc.CardBody([
                        html.H6(row["name"], className="text-muted mb-1"),
                        html.H4(f"${row['price_usd']:,.2f}", className="fw-bold mb-1"),
                        html.Span(
                            f"{arrow} {abs(row['price_change_24h']):.2f}%",
                            className=f"text-{color} fw-bold"
                        )
                    ])
                ], className="text-center h-100"),
                xs=6, md=4, lg=2
            )
        )

    # Market Cap Chart
    fig_mc = go.Figure(go.Bar(
        x=df["name"], y=df["market_cap"],
        marker_color="#7B61FF",
        text=df["market_cap"].apply(lambda x: f"${x/1e9:.1f}B"),
        textposition="outside"
    ))
    fig_mc.update_layout(
        paper_bgcolor="rgba(0,0,0,0)",
        plot_bgcolor="rgba(0,0,0,0)",
        font_color="white",
        margin=dict(t=20, b=20),
        yaxis=dict(showgrid=False, showticklabels=False),
        xaxis=dict(showgrid=False)
    )

    # Volume Chart
    fig_vol = go.Figure(go.Bar(
        x=df["name"], y=df["volume_24h"],
        marker_color="#00D4AA",
        text=df["volume_24h"].apply(lambda x: f"${x/1e9:.1f}B"),
        textposition="outside"
    ))
    fig_vol.update_layout(
        paper_bgcolor="rgba(0,0,0,0)",
        plot_bgcolor="rgba(0,0,0,0)",
        font_color="white",
        margin=dict(t=20, b=20),
        yaxis=dict(showgrid=False, showticklabels=False),
        xaxis=dict(showgrid=False)
    )

    return cards, fig_mc, fig_vol


@app.callback(
    Output("price-history-chart", "figure"),
    Input("coin-selector", "value"),
    Input("interval", "n_intervals")
)
def update_history(coin_id, n):
    df = get_price_history(coin_id)

    fig = go.Figure(go.Scatter(
        x=df["fetched_at"], y=df["price_usd"],
        mode="lines+markers",
        line=dict(color="#7B61FF", width=2),
        marker=dict(size=4),
        fill="tozeroy",
        fillcolor="rgba(123, 97, 255, 0.1)"
    ))
    fig.update_layout(
        paper_bgcolor="rgba(0,0,0,0)",
        plot_bgcolor="rgba(0,0,0,0)",
        font_color="white",
        margin=dict(t=20, b=20),
        xaxis=dict(showgrid=False),
        yaxis=dict(showgrid=True, gridcolor="rgba(255,255,255,0.1)")
    )
    return fig


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8050, debug=False)