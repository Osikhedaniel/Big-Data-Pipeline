import streamlit as st
from kafka import KafkaConsumer
import json
import pandas as pd
import time
import plotly.express as px
import plotly.graph_objects as go

st.set_page_config(
    page_title="Real-Time Logistics Dashboard",
    page_icon="🚚",
    layout="wide"
)

st.title("🚚 Real-Time Logistics Dashboard")

# Initialize Kafka consumer
@st.cache_resource
def get_consumer():
    return KafkaConsumer(
        "dashboard_stream",
        bootstrap_servers="localhost:9092",
        value_deserializer=lambda x: json.loads(x.decode("utf-8")),
        auto_offset_reset="latest",
        enable_auto_commit=True,
        consumer_timeout_ms=1000
    )

try:
    consumer = get_consumer()
except Exception as e:
    st.error(f"Failed to connect to Kafka: {e}")
    st.stop()

# Initialize session state for data
if 'data_list' not in st.session_state:
    st.session_state.data_list = []
if 'last_update' not in st.session_state:
    st.session_state.last_update = time.time()

# Function to convert data types
def convert_data_types(df):
    """Convert string columns to proper data types"""
    if df.empty:
        return df
    
    # Convert numeric columns
    numeric_columns = ['latitude', 'longitude', 'speed', 'eta_minutes']
    for col in numeric_columns:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors='coerce')
    
    # Convert boolean columns
    if 'delay_flag' in df.columns:
        df['delay_flag'] = df['delay_flag'].astype(bool)
    
    # Convert timestamp
    if 'timestamp' in df.columns:
        try:
            df['timestamp'] = pd.to_datetime(df['timestamp'])
        except:
            pass
    
    return df

# Create placeholders for dynamic content
main_placeholder = st.empty()

# Continuous update loop
while True:
    try:
        # Poll for messages
        messages = consumer.poll(timeout_ms=1000)
        
        for _, msgs in messages.items():
            for message in msgs:
                st.session_state.data_list.append(message.value)
        
        # Limit data to last 1000 records for performance
        if len(st.session_state.data_list) > 1000:
            st.session_state.data_list = st.session_state.data_list[-1000:]
        
        # Create dashboard if we have data
        if st.session_state.data_list:
            df = pd.DataFrame(st.session_state.data_list)
            
            # Convert data types
            df = convert_data_types(df)
            
            # Drop rows with invalid data
            df = df.dropna(subset=['speed'])
            
            with main_placeholder.container():
                
                # Key metrics
                st.subheader("Key Metrics")
                
                col1, col2, col3, col4, col5 = st.columns(5)
                
                with col1:
                    st.metric(
                        "Total Events",
                        f"{len(df):,}",
                        help="Total number of GPS updates received"
                    )
                
                with col2:
                    avg_speed = round(df["speed"].mean(), 2) if not df.empty and not df["speed"].isna().all() else 0
                    st.metric(
                        "Average Speed",
                        f"{avg_speed} km/h",
                        help="Average speed across all vehicles"
                    )
                
                with col3:
                    unique_vehicles = df["vehicle_id"].nunique() if not df.empty else 0
                    st.metric(
                        "Active Vehicles",
                        unique_vehicles,
                        help="Number of unique vehicles reporting"
                    )
                
                with col4:
                    if not df.empty and 'delay_flag' in df.columns:
                        delay_count = df["delay_flag"].sum() if df["delay_flag"].dtype == 'bool' else 0
                        delay_pct = round((delay_count / len(df)) * 100, 1) if not df.empty else 0
                        st.metric(
                            "Delays",
                            f"{delay_pct}%",
                            delta=f"{delay_count} events",
                            help="Percentage of delayed deliveries"
                        )
                    else:
                        st.metric("Delays", "N/A")
                
                with col5:
                    # Most common status
                    if not df.empty and 'status' in df.columns:
                        top_status = df['status'].mode().iloc[0] if not df['status'].mode().empty else "N/A"
                        status_count = df[df['status'] == top_status].shape[0] if top_status != "N/A" else 0
                        st.metric(
                            "Most Common Status",
                            top_status,
                            delta=f"{status_count} events",
                            help="Most frequent delivery status"
                        )
                    else:
                        st.metric("Most Common Status", "N/A")
                
                #Status breakdown
                st.subheader("Status Distribution")
                
                col1, col2 = st.columns([2, 1])
                
                with col1:
                    if not df.empty and 'status' in df.columns:
                        status_counts = df['status'].value_counts()
                        if not status_counts.empty:
                            fig_status = px.pie(
                                values=status_counts.values,
                                names=status_counts.index,
                                title="Delivery Status Distribution",
                                color_discrete_sequence=px.colors.qualitative.Set3
                            )
                            fig_status.update_traces(textposition='inside', textinfo='percent+label')
                            st.plotly_chart(fig_status, use_container_width=True)
                    else:
                        st.info("No status data available")
                
                with col2:
                    if not df.empty and 'delay_flag' in df.columns:
                        delay_counts = df['delay_flag'].value_counts()
                        if not delay_counts.empty:
                            fig_delay = px.pie(
                                values=delay_counts.values,
                                names=['On Time', 'Delayed'],
                                title="Delivery Performance",
                                color_discrete_sequence=['#00FF00', '#FF0000']
                            )
                            fig_delay.update_traces(textposition='inside', textinfo='percent+label')
                            st.plotly_chart(fig_delay, use_container_width=True)
                    else:
                        st.info("No delay data available")
                
                # Vehicle performance
                st.subheader("Vehicle Performance")
                
                if not df.empty and 'speed' in df.columns and not df['speed'].isna().all():
                    # Top vehicles by events
                    vehicle_stats = df.groupby('vehicle_id').agg({
                        'speed': ['mean', 'max', 'min']
                    }).round(2)
                    
                    vehicle_stats.columns = ['avg_speed', 'max_speed', 'min_speed']
                    vehicle_stats = vehicle_stats.reset_index()
                    
                    # Add delay info if available
                    if 'delay_flag' in df.columns:
                        delay_by_vehicle = df.groupby('vehicle_id')['delay_flag'].sum()
                        vehicle_stats = vehicle_stats.merge(
                            delay_by_vehicle.reset_index().rename(columns={'delay_flag': 'delays'}),
                            on='vehicle_id',
                            how='left'
                        )
                        vehicle_stats['delays'] = vehicle_stats['delays'].fillna(0)
                    
                    # Display as dataframe
                    st.dataframe(
                        vehicle_stats.head(10),
                        use_container_width=True,
                        hide_index=True
                    )
                    
                    # Speed distribution
                    if not df['speed'].isna().all():
                        fig_speed = px.histogram(
                            df,
                            x='speed',
                            nbins=20,
                            title="Speed Distribution",
                            labels={'speed': 'Speed (km/h)', 'count': 'Number of Events'},
                            color_discrete_sequence=['#1f77b4']
                        )
                        fig_speed.add_vline(
                            x=df['speed'].mean(), 
                            line_dash="dash", 
                            line_color="red",
                            annotation_text=f"Mean: {df['speed'].mean():.1f} km/h"
                        )
                        st.plotly_chart(fig_speed, use_container_width=True)
                else:
                    st.info("No vehicle data available")
                
                # Geographic analysis
                st.subheader("Geographic Distribution")
                
                if not df.empty and 'latitude' in df.columns and 'longitude' in df.columns:
                    # Drop rows with invalid coordinates
                    valid_coords = df.dropna(subset=['latitude', 'longitude'])
                    
                    if not valid_coords.empty:
                        col1, col2 = st.columns(2)
                        
                        with col1:
                            # Scatter map of vehicle locations
                            fig_map = px.scatter_mapbox(
                                valid_coords,
                                lat='latitude',
                                lon='longitude',
                                color='status' if 'status' in valid_coords.columns else None,
                                size='speed' if 'speed' in valid_coords.columns else None,
                                hover_name='vehicle_id' if 'vehicle_id' in valid_coords.columns else None,
                                hover_data=['driver_name' if 'driver_name' in valid_coords.columns else None,
                                          'speed' if 'speed' in valid_coords.columns else None,
                                          'status' if 'status' in valid_coords.columns else None],
                                title="Vehicle Locations",
                                color_discrete_sequence=px.colors.qualitative.Set1,
                                zoom=8,
                                height=400
                            )
                            fig_map.update_layout(mapbox_style="open-street-map")
                            st.plotly_chart(fig_map, use_container_width=True)
                        
                        with col2:
                            # Geographic spread metrics
                            lat_center = valid_coords['latitude'].mean()
                            lon_center = valid_coords['longitude'].mean()
                            lat_span = valid_coords['latitude'].max() - valid_coords['latitude'].min()
                            lon_span = valid_coords['longitude'].max() - valid_coords['longitude'].min()
                            
                            st.metric("Center Latitude", f"{lat_center:.4f}" if not pd.isna(lat_center) else "N/A")
                            st.metric("Center Longitude", f"{lon_center:.4f}" if not pd.isna(lon_center) else "N/A")
                            st.metric("Latitude Range", f"{lat_span:.4f}" if not pd.isna(lat_span) else "N/A")
                            st.metric("Longitude Range", f"{lon_span:.4f}" if not pd.isna(lon_span) else "N/A")
                else:
                    st.info("No geographic data available")
                
                #Real-time tracking
                st.subheader("Live Vehicle Tracking")
                
                # Show latest events
                display_df = df.tail(20)
                # Format for display
                if 'speed' in display_df.columns:
                    display_df['speed'] = display_df['speed'].round(2)
                if 'eta_minutes' in display_df.columns:
                    display_df['eta_minutes'] = display_df['eta_minutes'].round(2)
                
                st.dataframe(
                    display_df,
                    use_container_width=True,
                    hide_index=True
                )
                
                # Time series analysis
                if not df.empty and 'timestamp' in df.columns:
                    try:
                        st.subheader("Time Series Analysis")
                        
                        # Group by minute for trends
                        df['minute'] = df['timestamp'].dt.floor('min')
                        minute_metrics = df.groupby('minute').agg({
                            'speed': 'mean' if 'speed' in df.columns else None,
                            'vehicle_id': 'count'
                        }).rename(columns={'vehicle_id': 'event_count'})
                        
                        if not minute_metrics.empty:
                            col1, col2 = st.columns(2)
                            
                            with col1:
                                fig_trend = px.line(
                                    minute_metrics.reset_index(),
                                    x='minute',
                                    y='event_count',
                                    title="Event Rate Over Time",
                                    labels={'event_count': 'Events per Minute', 'minute': 'Time'}
                                )
                                st.plotly_chart(fig_trend, use_container_width=True)
                            
                            with col2:
                                if 'speed' in minute_metrics.columns:
                                    fig_speed_trend = px.line(
                                        minute_metrics.reset_index(),
                                        x='minute',
                                        y='speed',
                                        title="Average Speed Over Time",
                                        labels={'speed': 'Speed (km/h)', 'minute': 'Time'}
                                    )
                                    st.plotly_chart(fig_speed_trend, use_container_width=True)
                    except Exception as e:
                        st.info(f"Time series data not available: {e}")
                
        else:
            with main_placeholder.container():
                st.info("🔄 Waiting for vehicle data...")
        
        # Update timestamp
        st.session_state.last_update = time.time()
        
        time.sleep(3)
        
    except Exception as e:
        st.error(f"Error: {e}")
        time.sleep(1)