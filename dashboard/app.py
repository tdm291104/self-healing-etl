"""
Self-Healing ETL — Agent Dashboard (Phase 4)
Shows agent audit trail and DQ check history from monitoring schema.
"""

import contextlib
import json
import os
from datetime import datetime

import pandas as pd
import plotly.express as px
import psycopg2
import streamlit as st

# ── Config ───────────────────────────────────────────────────────────────────

DB_CONFIG = {
    'host':     os.getenv('DB_HOST', 'localhost'),
    'port':     int(os.getenv('DB_PORT', '5432')),
    'dbname':   os.getenv('DB_NAME', 'crypto_dw'),
    'user':     os.getenv('DB_USER', 'dbt_user'),
    'password': os.getenv('DB_PASSWORD', 'dbt_password'),
}

# Status color palette — pass/fail/warning are status semantics, not categorical
STATUS_COLOR = {
    'pass':    '#22c55e',
    'warning': '#f59e0b',
    'fail':    '#ef4444',
}
DECISION_COLOR = {
    'auto_fix':       '#22c55e',
    'needs_approval': '#f59e0b',
    'escalate':       '#ef4444',
    'no_action':      '#94a3b8',
}
DECISION_ICON = {
    'auto_fix':       '✅',
    'needs_approval': '⚠️',
    'escalate':       '🚨',
    'no_action':      '—',
}


# ── Data loading ─────────────────────────────────────────────────────────────

@st.cache_data(ttl=30)
def load_agent_actions() -> pd.DataFrame:
    with contextlib.closing(psycopg2.connect(**DB_CONFIG)) as conn:
        return pd.read_sql(
            """
            SELECT id, timestamp, trigger_type, diagnosis, decision,
                   action_taken, confidence_score, human_feedback
            FROM monitoring.agent_actions
            ORDER BY timestamp DESC
            LIMIT 200
            """,
            conn,
        )


@st.cache_data(ttl=30)
def load_dq_results() -> pd.DataFrame:
    with contextlib.closing(psycopg2.connect(**DB_CONFIG)) as conn:
        df = pd.read_sql(
            """
            SELECT id, table_name, check_name, status, details, timestamp
            FROM monitoring.dq_check_results
            ORDER BY timestamp DESC
            LIMIT 500
            """,
            conn,
        )
    if not df.empty:
        df['date'] = pd.to_datetime(df['timestamp']).dt.date
        df['details_str'] = df['details'].apply(
            lambda x: json.dumps(x) if isinstance(x, dict) else str(x)
        )
    return df


def _fmt_confidence(val) -> str:
    try:
        return f"{float(val):.0%}"
    except (TypeError, ValueError):
        return '—'


def _color_cell(val, color_map: dict, alpha: str = '22') -> str:
    c = color_map.get(str(val), '#94a3b8')
    return f'background-color: {c}{alpha}; color: {c}; font-weight: 600'


# ── Page setup ────────────────────────────────────────────────────────────────

st.set_page_config(
    page_title='Self-Healing ETL',
    page_icon='🔧',
    layout='wide',
)

st.title('🔧 Self-Healing ETL — Agent Dashboard')

col_hdr_l, col_hdr_r = st.columns([3, 1])
with col_hdr_l:
    st.caption(f"Auto-refreshes every 30 s — last loaded {datetime.now().strftime('%H:%M:%S')}")
with col_hdr_r:
    if st.button('↻ Refresh now'):
        st.cache_data.clear()
        st.rerun()

# ── Load data ─────────────────────────────────────────────────────────────────

try:
    actions = load_agent_actions()
    dq = load_dq_results()
    db_ok = True
except Exception as e:
    st.error(f"Cannot connect to database: {e}")
    st.stop()

# ── KPI row ───────────────────────────────────────────────────────────────────

k1, k2, k3, k4 = st.columns(4)

total_runs = len(actions)
auto_fixes = int((actions['decision'] == 'auto_fix').sum()) if total_runs else 0
escalations = int((actions['decision'] == 'escalate').sum()) if total_runs else 0
avg_conf = actions['confidence_score'].astype(float).mean() if total_runs else None
total_dq_fail = int((dq['status'] == 'fail').sum()) if not dq.empty else 0

k1.metric('Agent Runs', total_runs)
k2.metric('Auto-Fixed', auto_fixes)
k3.metric('Escalated', escalations,
          delta=str(escalations) if escalations else None,
          delta_color='inverse')
k4.metric('Avg Confidence', f'{avg_conf:.0%}' if avg_conf else '—')

st.divider()

# ── Charts row ────────────────────────────────────────────────────────────────

chart_l, chart_r = st.columns(2)

with chart_l:
    st.subheader('Agent Decisions')
    if total_runs:
        dec = (
            actions['decision']
            .value_counts()
            .reset_index()
            .rename(columns={'index': 'decision', 'decision': 'count',
                             'count': 'count'})
        )
        # pandas value_counts returns different column names depending on version
        dec.columns = ['decision', 'count']
        dec['label'] = dec['decision'].map(
            lambda d: f"{DECISION_ICON.get(d, '')} {d}"
        )
        fig = px.bar(
            dec, x='decision', y='count',
            color='decision',
            color_discrete_map=DECISION_COLOR,
            text='count',
            labels={'decision': '', 'count': 'runs'},
        )
        fig.update_traces(textposition='outside', marker_line_width=0)
        fig.update_layout(
            showlegend=False,
            margin=dict(t=10, b=10, l=0, r=0),
            plot_bgcolor='rgba(0,0,0,0)',
            paper_bgcolor='rgba(0,0,0,0)',
        )
        st.plotly_chart(fig, use_container_width=True)
    else:
        st.info('No agent runs yet — trigger the monitor_agent DAG.')

with chart_r:
    st.subheader('DQ Checks by Status')
    if not dq.empty:
        grp = (
            dq.groupby(['check_name', 'status'])
            .size()
            .reset_index(name='count')
        )
        fig2 = px.bar(
            grp, x='check_name', y='count',
            color='status',
            color_discrete_map=STATUS_COLOR,
            barmode='stack',
            labels={'check_name': '', 'count': 'checks', 'status': 'Status'},
        )
        fig2.update_layout(
            margin=dict(t=10, b=10, l=0, r=0),
            xaxis_tickangle=-15,
            plot_bgcolor='rgba(0,0,0,0)',
            paper_bgcolor='rgba(0,0,0,0)',
            legend=dict(orientation='h', yanchor='bottom', y=1.02, xanchor='right', x=1),
        )
        st.plotly_chart(fig2, use_container_width=True)
    else:
        st.info('No DQ results yet — trigger the ingest_trades DAG.')

# ── DQ failures over time ─────────────────────────────────────────────────────

if not dq.empty and dq['date'].nunique() > 1:
    st.subheader('DQ Failures Over Time')
    daily = (
        dq[dq['status'] == 'fail']
        .groupby(['date', 'check_name'])
        .size()
        .reset_index(name='failures')
    )
    fig3 = px.line(
        daily, x='date', y='failures',
        color='check_name',
        markers=True,
        labels={'date': 'Date', 'failures': 'Failures', 'check_name': 'Check'},
    )
    fig3.update_traces(line_width=2, marker_size=8)
    fig3.update_layout(
        margin=dict(t=10, b=10, l=0, r=0),
        plot_bgcolor='rgba(0,0,0,0)',
        paper_bgcolor='rgba(0,0,0,0)',
    )
    st.plotly_chart(fig3, use_container_width=True)

st.divider()

# ── Agent audit trail ─────────────────────────────────────────────────────────

st.subheader('Agent Audit Trail')
if total_runs:
    display_actions = actions[[
        'timestamp', 'decision', 'confidence_score',
        'diagnosis', 'action_taken', 'human_feedback',
    ]].copy()
    display_actions['confidence_score'] = display_actions['confidence_score'].apply(
        _fmt_confidence
    )
    display_actions = display_actions.rename(columns={
        'timestamp': 'Time',
        'decision': 'Decision',
        'confidence_score': 'Confidence',
        'diagnosis': 'Diagnosis',
        'action_taken': 'Action',
        'human_feedback': 'Feedback',
    })
    st.dataframe(
        display_actions.style.map(
            lambda v: _color_cell(v, DECISION_COLOR), subset=['Decision']
        ),
        use_container_width=True,
        hide_index=True,
    )
else:
    st.info('No agent actions yet.')

st.divider()

# ── DQ check history ─────────────────────────────────────────────────────────

st.subheader('DQ Check History')
if not dq.empty:
    status_filter = st.multiselect(
        'Filter by status',
        options=['fail', 'warning', 'pass'],
        default=['fail', 'warning'],
    )
    filtered_dq = dq[dq['status'].isin(status_filter)] if status_filter else dq

    display_dq = filtered_dq[[
        'timestamp', 'table_name', 'check_name', 'status', 'details_str',
    ]].rename(columns={
        'timestamp': 'Time',
        'table_name': 'Table',
        'check_name': 'Check',
        'status': 'Status',
        'details_str': 'Details',
    })
    st.dataframe(
        display_dq.style.map(
            lambda v: _color_cell(v, STATUS_COLOR), subset=['Status']
        ),
        use_container_width=True,
        hide_index=True,
    )
else:
    st.info('No DQ check results yet.')
