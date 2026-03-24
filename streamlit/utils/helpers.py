import streamlit as st

def format_number(value):
    """Formata números para exibição"""
    if value >= 1_000_000:
        return f"{value/1_000_000:.1f}M"
    elif value >= 1_000:
        return f"{value/1_000:.1f}K"
    else:
        return f"{value:.0f}"

def display_metric_card(title, value, unit="", delta=None):
    """Exibe card de métrica"""
    formatted_value = format_number(value)
    
    col1, col2 = st.columns([3, 1])
    with col1:
        st.metric(
            label=title,
            value=f"{formatted_value} {unit}",
            delta=delta
        )