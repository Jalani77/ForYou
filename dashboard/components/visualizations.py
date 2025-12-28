"""
Visualization Components Module
Professional visualization components with consistent styling and interactivity.
"""
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import pandas as pd
import streamlit as st
from typing import Optional
import logging

logger = logging.getLogger(__name__)


class VisualizationComponents:
    """
    Enterprise-grade visualization components with professional dark theme styling.
    All charts follow consistent design patterns and color schemes.
    """
    
    # Professional dark theme color palette
    COLORS = {
        'primary': '#00D4FF',
        'secondary': '#FF6B9D',
        'success': '#00E396',
        'warning': '#FEB019',
        'danger': '#FF4560',
        'info': '#775DD0',
        'dark_bg': '#1E1E1E',
        'card_bg': '#2D2D2D',
        'text': '#E0E0E0',
        'grid': '#404040'
    }
    
    # Color scales for different visualizations
    COLOR_SCALES = {
        'sequential': ['#1a1a2e', '#16213e', '#0f3460', '#533483', '#e94560'],
        'diverging': ['#00D4FF', '#00A8CC', '#775DD0', '#FF6B9D', '#FF4560'],
        'categorical': ['#00D4FF', '#00E396', '#FEB019', '#FF6B9D', '#775DD0', '#FF4560']
    }
    
    @staticmethod
    def get_base_layout(title: str, height: int = 400) -> dict:
        """
        Get base layout configuration for all charts.
        
        Args:
            title: Chart title
            height: Chart height in pixels
            
        Returns:
            dict: Plotly layout configuration
        """
        return {
            'title': {
                'text': title,
                'font': {'size': 18, 'color': VisualizationComponents.COLORS['text'], 'family': 'Arial, sans-serif'},
                'x': 0.02,
                'xanchor': 'left'
            },
            'paper_bgcolor': VisualizationComponents.COLORS['card_bg'],
            'plot_bgcolor': VisualizationComponents.COLORS['dark_bg'],
            'font': {'color': VisualizationComponents.COLORS['text'], 'family': 'Arial, sans-serif'},
            'height': height,
            'margin': {'l': 60, 'r': 40, 't': 80, 'b': 60},
            'xaxis': {
                'gridcolor': VisualizationComponents.COLORS['grid'],
                'color': VisualizationComponents.COLORS['text']
            },
            'yaxis': {
                'gridcolor': VisualizationComponents.COLORS['grid'],
                'color': VisualizationComponents.COLORS['text']
            },
            'hovermode': 'closest',
            'showlegend': True,
            'legend': {
                'bgcolor': 'rgba(0,0,0,0)',
                'bordercolor': VisualizationComponents.COLORS['grid'],
                'borderwidth': 1
            }
        }
    
    @staticmethod
    def create_metric_cards(metrics: dict):
        """
        Create professional metric cards with icons and delta indicators.
        
        Args:
            metrics: Dictionary of metrics with keys: label, value, delta, icon
        """
        cols = st.columns(len(metrics))
        
        for col, (key, metric) in zip(cols, metrics.items()):
            with col:
                # Custom styled metric card
                delta_color = VisualizationComponents.COLORS['success'] if metric.get('delta', 0) >= 0 else VisualizationComponents.COLORS['danger']
                delta_symbol = '▲' if metric.get('delta', 0) >= 0 else '▼'
                
                st.markdown(f"""
                    <div style="
                        background: linear-gradient(135deg, {VisualizationComponents.COLORS['card_bg']} 0%, {VisualizationComponents.COLORS['dark_bg']} 100%);
                        padding: 20px;
                        border-radius: 12px;
                        border-left: 4px solid {VisualizationComponents.COLORS['primary']};
                        box-shadow: 0 4px 6px rgba(0, 0, 0, 0.3);
                    ">
                        <div style="color: {VisualizationComponents.COLORS['text']}; opacity: 0.7; font-size: 14px; margin-bottom: 8px;">
                            {metric.get('icon', '📊')} {metric['label']}
                        </div>
                        <div style="color: {VisualizationComponents.COLORS['text']}; font-size: 28px; font-weight: bold; margin-bottom: 8px;">
                            {metric['value']}
                        </div>
                        <div style="color: {delta_color}; font-size: 14px;">
                            {delta_symbol} {metric.get('delta', 'N/A')}
                        </div>
                    </div>
                """, unsafe_allow_html=True)
    
    @staticmethod
    def create_multi_line_chart(df: pd.DataFrame,
                                x_col: str,
                                y_col: str,
                                group_col: str,
                                title: str = "Multi-Line Trend",
                                height: int = 500) -> go.Figure:
        """
        Create professional multi-line chart for credit consumption trends.
        
        Args:
            df: DataFrame with data
            x_col: Column name for x-axis
            y_col: Column name for y-axis
            group_col: Column name for grouping lines
            title: Chart title
            height: Chart height
            
        Returns:
            go.Figure: Plotly figure object
        """
        if df.empty:
            return VisualizationComponents._create_empty_chart(title, height)
        
        fig = go.Figure()
        
        # Get unique groups and assign colors
        groups = df[group_col].unique()
        colors = VisualizationComponents.COLOR_SCALES['categorical']
        
        for i, group in enumerate(groups[:10]):  # Limit to 10 lines for readability
            group_data = df[df[group_col] == group]
            
            fig.add_trace(go.Scatter(
                x=group_data[x_col],
                y=group_data[y_col],
                mode='lines+markers',
                name=str(group),
                line=dict(
                    color=colors[i % len(colors)],
                    width=3
                ),
                marker=dict(
                    size=6,
                    line=dict(width=1, color='white')
                ),
                hovertemplate=(
                    f'<b>{group}</b><br>' +
                    f'{x_col}: %{{x}}<br>' +
                    f'{y_col}: %{{y:,.0f}}<br>' +
                    '<extra></extra>'
                )
            ))
        
        layout = VisualizationComponents.get_base_layout(title, height)
        layout['xaxis']['title'] = x_col.replace('_', ' ').title()
        layout['yaxis']['title'] = y_col.replace('_', ' ').title()
        layout['legend']['orientation'] = 'v'
        layout['legend']['yanchor'] = 'top'
        layout['legend']['y'] = 1
        layout['legend']['xanchor'] = 'left'
        layout['legend']['x'] = 1.02
        
        fig.update_layout(**layout)
        
        return fig
    
    @staticmethod
    def create_segmented_bar_chart(df: pd.DataFrame,
                                   x_col: str,
                                   y_col: str,
                                   segment_col: str,
                                   title: str = "Segmented Analysis",
                                   height: int = 450) -> go.Figure:
        """
        Create professional segmented bar chart for churn risk by tier.
        
        Args:
            df: DataFrame with data
            x_col: Column name for x-axis (categories)
            y_col: Column name for y-axis (values)
            segment_col: Column name for segments
            title: Chart title
            height: Chart height
            
        Returns:
            go.Figure: Plotly figure object
        """
        if df.empty:
            return VisualizationComponents._create_empty_chart(title, height)
        
        # Pivot data for stacked bar chart
        pivot_df = df.pivot_table(
            index=x_col,
            columns=segment_col,
            values=y_col,
            aggfunc='sum',
            fill_value=0
        )
        
        fig = go.Figure()
        
        # Define colors for risk categories
        risk_colors = {
            'High Risk': VisualizationComponents.COLORS['danger'],
            'Medium Risk': VisualizationComponents.COLORS['warning'],
            'Low Risk': VisualizationComponents.COLORS['info'],
            'Healthy': VisualizationComponents.COLORS['success']
        }
        
        for segment in pivot_df.columns:
            color = risk_colors.get(segment, VisualizationComponents.COLORS['primary'])
            
            fig.add_trace(go.Bar(
                name=segment,
                x=pivot_df.index,
                y=pivot_df[segment],
                marker=dict(
                    color=color,
                    line=dict(color='rgba(0,0,0,0.3)', width=1)
                ),
                hovertemplate=(
                    f'<b>{segment}</b><br>' +
                    f'{x_col}: %{{x}}<br>' +
                    f'Count: %{{y}}<br>' +
                    '<extra></extra>'
                )
            ))
        
        layout = VisualizationComponents.get_base_layout(title, height)
        layout['barmode'] = 'stack'
        layout['xaxis']['title'] = x_col.replace('_', ' ').title()
        layout['yaxis']['title'] = 'Customer Count'
        
        fig.update_layout(**layout)
        
        return fig
    
    @staticmethod
    def create_health_score_gauge(health_score: float,
                                  customer_name: str = "Customer") -> go.Figure:
        """
        Create gauge chart for customer health score.
        
        Args:
            health_score: Health score value (0-100)
            customer_name: Customer name for title
            
        Returns:
            go.Figure: Plotly figure object
        """
        # Determine color based on score
        if health_score >= 80:
            color = VisualizationComponents.COLORS['success']
            status = 'Healthy'
        elif health_score >= 60:
            color = VisualizationComponents.COLORS['warning']
            status = 'At Risk'
        else:
            color = VisualizationComponents.COLORS['danger']
            status = 'Critical'
        
        fig = go.Figure(go.Indicator(
            mode="gauge+number+delta",
            value=health_score,
            domain={'x': [0, 1], 'y': [0, 1]},
            title={'text': f"{customer_name}<br><span style='font-size:0.8em;color:gray'>{status}</span>"},
            gauge={
                'axis': {'range': [None, 100], 'tickwidth': 1, 'tickcolor': VisualizationComponents.COLORS['text']},
                'bar': {'color': color, 'thickness': 0.75},
                'bgcolor': VisualizationComponents.COLORS['dark_bg'],
                'borderwidth': 2,
                'bordercolor': VisualizationComponents.COLORS['grid'],
                'steps': [
                    {'range': [0, 60], 'color': 'rgba(255, 69, 96, 0.3)'},
                    {'range': [60, 80], 'color': 'rgba(254, 176, 25, 0.3)'},
                    {'range': [80, 100], 'color': 'rgba(0, 227, 150, 0.3)'}
                ],
                'threshold': {
                    'line': {'color': "white", 'width': 4},
                    'thickness': 0.75,
                    'value': health_score
                }
            }
        ))
        
        fig.update_layout(
            paper_bgcolor=VisualizationComponents.COLORS['card_bg'],
            font={'color': VisualizationComponents.COLORS['text']},
            height=300,
            margin={'l': 20, 'r': 20, 't': 60, 'b': 20}
        )
        
        return fig
    
    @staticmethod
    def create_data_table(df: pd.DataFrame,
                         title: str = "Data Table",
                         max_rows: int = 10) -> None:
        """
        Create professional styled data table.
        
        Args:
            df: DataFrame to display
            title: Table title
            max_rows: Maximum rows to display
        """
        if df.empty:
            st.info(f"📊 {title}: No data available")
            return
        
        # Style the dataframe
        st.markdown(f"""
            <div style="
                background: {VisualizationComponents.COLORS['card_bg']};
                padding: 20px;
                border-radius: 12px;
                margin: 10px 0;
                border-left: 4px solid {VisualizationComponents.COLORS['primary']};
            ">
                <h3 style="color: {VisualizationComponents.COLORS['text']}; margin-bottom: 15px;">
                    {title}
                </h3>
            </div>
        """, unsafe_allow_html=True)
        
        # Display table with custom styling
        st.dataframe(
            df.head(max_rows),
            use_container_width=True,
            height=min(400, 50 + 35 * len(df.head(max_rows)))
        )
    
    @staticmethod
    def create_revenue_donut_chart(df: pd.DataFrame,
                                   values_col: str,
                                   names_col: str,
                                   title: str = "Revenue Distribution",
                                   height: int = 400) -> go.Figure:
        """
        Create professional donut chart for revenue distribution.
        
        Args:
            df: DataFrame with data
            values_col: Column name for values
            names_col: Column name for labels
            title: Chart title
            height: Chart height
            
        Returns:
            go.Figure: Plotly figure object
        """
        if df.empty:
            return VisualizationComponents._create_empty_chart(title, height)
        
        fig = go.Figure(data=[go.Pie(
            labels=df[names_col],
            values=df[values_col],
            hole=0.5,
            marker=dict(
                colors=VisualizationComponents.COLOR_SCALES['categorical'],
                line=dict(color=VisualizationComponents.COLORS['dark_bg'], width=2)
            ),
            textposition='outside',
            textinfo='label+percent',
            hovertemplate=(
                '<b>%{label}</b><br>' +
                'Revenue: $%{value:,.0f}<br>' +
                'Percentage: %{percent}<br>' +
                '<extra></extra>'
            )
        )])
        
        layout = VisualizationComponents.get_base_layout(title, height)
        layout['showlegend'] = True
        layout['legend']['orientation'] = 'v'
        
        fig.update_layout(**layout)
        
        return fig
    
    @staticmethod
    def create_heatmap(df: pd.DataFrame,
                      x_col: str,
                      y_col: str,
                      value_col: str,
                      title: str = "Heatmap",
                      height: int = 500) -> go.Figure:
        """
        Create professional heatmap visualization.
        
        Args:
            df: DataFrame with data
            x_col: Column for x-axis
            y_col: Column for y-axis
            value_col: Column for color values
            title: Chart title
            height: Chart height
            
        Returns:
            go.Figure: Plotly figure object
        """
        if df.empty:
            return VisualizationComponents._create_empty_chart(title, height)
        
        # Pivot data for heatmap
        pivot_df = df.pivot_table(
            index=y_col,
            columns=x_col,
            values=value_col,
            aggfunc='sum',
            fill_value=0
        )
        
        fig = go.Figure(data=go.Heatmap(
            z=pivot_df.values,
            x=pivot_df.columns,
            y=pivot_df.index,
            colorscale='Viridis',
            hovertemplate=(
                f'{x_col}: %{{x}}<br>' +
                f'{y_col}: %{{y}}<br>' +
                f'{value_col}: %{{z:,.0f}}<br>' +
                '<extra></extra>'
            ),
            colorbar=dict(
                title=dict(
                    text=value_col.replace('_', ' ').title(),
                    font=dict(color=VisualizationComponents.COLORS['text'])
                ),
                tickfont=dict(color=VisualizationComponents.COLORS['text'])
            )
        ))
        
        layout = VisualizationComponents.get_base_layout(title, height)
        layout['xaxis']['title'] = x_col.replace('_', ' ').title()
        layout['yaxis']['title'] = y_col.replace('_', ' ').title()
        
        fig.update_layout(**layout)
        
        return fig
    
    @staticmethod
    def _create_empty_chart(title: str, height: int) -> go.Figure:
        """
        Create empty chart with message.
        
        Args:
            title: Chart title
            height: Chart height
            
        Returns:
            go.Figure: Empty figure with message
        """
        fig = go.Figure()
        
        fig.add_annotation(
            text="No data available",
            xref="paper",
            yref="paper",
            x=0.5,
            y=0.5,
            showarrow=False,
            font=dict(size=20, color=VisualizationComponents.COLORS['text'])
        )
        
        layout = VisualizationComponents.get_base_layout(title, height)
        fig.update_layout(**layout)
        
        return fig
