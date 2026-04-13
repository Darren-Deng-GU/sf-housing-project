"""
Generate all interactive charts for SF Housing Project.
Reads from: C:/sf-housing/data/processed/
Writes to:  C:/sf-housing/docs/charts/
"""
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import os

# Setup
DATA = "C:/sf-housing/data/processed"
OUT = "C:/sf-housing/docs/charts"
os.makedirs(OUT, exist_ok=True)

PALETTE = ["#004777", "#A30000", "#FF7700", "#EFD28D", "#00AFB5",
           "#6A4C93", "#1982C4", "#8AC926", "#FFCA3A", "#FF595E"]
FONT = "Inter, Arial, sans-serif"
TITLE_COLOR = "#004777"

def save(fig, name):
    path = os.path.join(OUT, name)
    fig.write_html(path, include_plotlyjs="cdn", full_html=True,
                   config={"displayModeBar": True, "displaylogo": False})
    print(f"  OK  {name} ({os.path.getsize(path)//1024} KB)")

def style(fig, title, h=550):
    fig.update_layout(
        title=dict(text=f"<b>{title}</b>",
                   font=dict(family=FONT, size=18, color=TITLE_COLOR), x=0.02),
        font=dict(family=FONT, size=13),
        template="plotly_white", height=h,
        margin=dict(l=60, r=40, t=70, b=60),
        plot_bgcolor="white", paper_bgcolor="white")
    return fig

# ── 1. Housing Types ──
print("\n1. Housing Types")
df = pd.read_csv(f"{DATA}/housing_types.csv")
df.columns = [c.strip() for c in df.columns]
df = df.sort_values(df.columns[1], ascending=False)
fig = px.bar(df, x=df.columns[0], y=df.columns[1], color=df.columns[0],
             color_discrete_sequence=PALETTE, text=df.columns[1])
fig.update_traces(texttemplate="%{text:,}", textposition="outside")
fig.update_layout(showlegend=False, xaxis_tickangle=-30)
style(fig, "Building Permits by Property Use Type")
save(fig, "housing_types.html")

# ── 2. Permits Trend ──
print("2. Permits Trend")
df = pd.read_csv(f"{DATA}/permits_trend.csv")
df.columns = [c.strip() for c in df.columns]
# Remove rows with empty year
df = df.dropna(subset=[df.columns[0]])
df[df.columns[0]] = df[df.columns[0]].astype(int)
# Top 5 types
top = df.groupby(df.columns[1])[df.columns[2]].sum().nlargest(5).index.tolist()
df2 = df[df[df.columns[1]].isin(top)]
fig = px.line(df2, x=df2.columns[0], y=df2.columns[2], color=df2.columns[1],
              markers=True, color_discrete_sequence=PALETTE)
fig.update_traces(line=dict(width=2.5))
style(fig, "Building Permits Issued by Type (2000-2025)")
fig.update_layout(legend=dict(orientation="h", yanchor="bottom", y=-0.35, x=0.5, xanchor="center"))
save(fig, "permits_trend.html")

# ── 3. Change of Use (Sankey) ──
print("3. Change of Use")
df = pd.read_csv(f"{DATA}/change_of_use.csv")
df.columns = [c.strip() for c in df.columns]
sources = df[df.columns[0]].unique().tolist()
targets = df[df.columns[1]].unique().tolist()
all_nodes = sources + [t for t in targets if t not in sources]
idx = {n: i for i, n in enumerate(all_nodes)}
n = len(all_nodes)
colors = (PALETTE * ((n // len(PALETTE)) + 1))[:n]
def hex_to_rgba(h, a=0.4):
    return f"rgba({int(h[1:3],16)},{int(h[3:5],16)},{int(h[5:7],16)},{a})"
link_colors = [hex_to_rgba(colors[idx[row.iloc[0]]]) for _, row in df.iterrows()]
fig = go.Figure(go.Sankey(
    node=dict(pad=15, thickness=20, label=all_nodes, color=colors),
    link=dict(source=[idx[r] for r in df[df.columns[0]]],
              target=[idx[r] for r in df[df.columns[1]]],
              value=df[df.columns[2]].tolist(), color=link_colors)))
style(fig, "Top Changes in Building Use", 600)
save(fig, "change_of_use.html")

# ── 4. Processing Time (Box Plot) ──
print("4. Processing Time")
df = pd.read_csv(f"{DATA}/processing_time.csv")
df.columns = [c.strip() for c in df.columns]
counts = df[df.columns[0]].value_counts()
top6 = counts.nlargest(6).index.tolist()
df2 = df[df[df.columns[0]].isin(top6)]
fig = px.box(df2, x=df2.columns[0], y=df2.columns[1], color=df2.columns[0],
             color_discrete_sequence=PALETTE)
fig.update_layout(showlegend=False, xaxis_tickangle=-20)
style(fig, "Distribution of Permit Processing Times")
save(fig, "processing_time.html")

# ── 5. Neighborhoods (Horizontal Bar) ──
print("5. Neighborhoods")
df = pd.read_csv(f"{DATA}/neighborhood_activity.csv")
df.columns = [c.strip() for c in df.columns]
df = df.sort_values(df.columns[1], ascending=True)
fig = px.bar(df, x=df.columns[1], y=df.columns[0], orientation="h",
             text=df.columns[1], color=df.columns[1],
             color_continuous_scale=["#EFD28D", "#FF7700", "#A30000"])
fig.update_traces(texttemplate="%{text:,}", textposition="outside")
fig.update_layout(coloraxis_showscale=False)
style(fig, "Top 20 Neighborhoods by Permit Activity", 650)
save(fig, "neighborhoods.html")

# ── 6. House Price Index ──
print("6. HPI")
df = pd.read_csv(f"{DATA}/hpi_clean.csv")
df.columns = [c.strip() for c in df.columns]
df[df.columns[0]] = pd.to_datetime(df[df.columns[0]])
fig = px.line(df, x=df.columns[0], y=df.columns[1])
fig.update_traces(line=dict(color="#FF7700", width=2.5))
style(fig, "House Price Index: San Francisco MSA (1975=100)")
save(fig, "hpi.html")

# ── 7. Permits vs HPI (Dual Axis) ──
print("7. Permits vs HPI")
permits = pd.read_csv(f"{DATA}/permits_trend.csv")
permits.columns = [c.strip() for c in permits.columns]
permits = permits.dropna(subset=[permits.columns[0]])
permits[permits.columns[0]] = permits[permits.columns[0]].astype(int)
annual = permits.groupby(permits.columns[0])[permits.columns[2]].sum().reset_index()
annual.columns = ["year", "total_permits"]

hpi = pd.read_csv(f"{DATA}/hpi_clean.csv")
hpi.columns = [c.strip() for c in hpi.columns]
hpi[hpi.columns[0]] = pd.to_datetime(hpi[hpi.columns[0]])
hpi["year"] = hpi[hpi.columns[0]].dt.year
annual_hpi = hpi.groupby("year")[hpi.columns[1]].mean().reset_index()
annual_hpi.columns = ["year", "hpi"]

merged = annual.merge(annual_hpi, on="year")
merged = merged[(merged["year"] >= 2000) & (merged["year"] <= 2025)]

fig = go.Figure()
fig.add_trace(go.Bar(x=merged["year"], y=merged["total_permits"],
                     name="Total Permits", marker_color="#004777", opacity=0.7))
fig.add_trace(go.Scatter(x=merged["year"], y=merged["hpi"],
                         name="House Price Index", line=dict(color="#FF7700", width=3),
                         mode="lines+markers", yaxis="y2"))
fig.update_layout(
    yaxis=dict(title="Permits Issued", showgrid=False),
    yaxis2=dict(title="House Price Index", side="right", overlaying="y", showgrid=False),
    legend=dict(orientation="h", yanchor="bottom", y=-0.25, x=0.5, xanchor="center"))
style(fig, "Annual Permits vs. House Price Index (2000-2025)")
save(fig, "permits_vs_hpi.html")

# ── 8. Rent Map ──
print("8. Rent Map")
try:
    import folium
    import branca.colormap as cm

    df = pd.read_csv(f"{DATA}/census_clean.csv")
    df.columns = [c.strip() for c in df.columns]
    # Find correct column names
    rent_col = [c for c in df.columns if 'rent' in c.lower()][0]
    income_col = [c for c in df.columns if 'income' in c.lower()][0]
    pop_col = [c for c in df.columns if 'pop_total' in c.lower() or c.lower() == 'pop_total'][0]
    county_col = [c for c in df.columns if 'county' in c.lower()][0]

    county_names = {"001":"Alameda","013":"Contra Costa","041":"Marin","055":"Napa",
                    "075":"San Francisco","081":"San Mateo","085":"Santa Clara",
                    "095":"Solano","097":"Sonoma"}
    county_coords = {"001":(37.60,-121.72),"013":(37.95,-122.03),"041":(38.08,-122.76),
                     "055":(38.50,-122.27),"075":(37.77,-122.42),"081":(37.43,-122.40),
                     "085":(37.35,-121.96),"095":(38.25,-122.04),"097":(38.51,-122.98)}

    df[rent_col] = pd.to_numeric(df[rent_col], errors="coerce")
    df[income_col] = pd.to_numeric(df[income_col], errors="coerce")
    df[pop_col] = pd.to_numeric(df[pop_col], errors="coerce")
    df[county_col] = df[county_col].astype(str).str.zfill(3)

    stats = df.groupby(county_col).agg(
        med_rent=(rent_col, "median"),
        med_income=(income_col, "median"),
        population=(pop_col, "sum"),
        tracts=(county_col, "count")).reset_index()

    m = folium.Map(location=[37.75,-122.25], zoom_start=9, tiles="CartoDB positron")
    cmap = cm.LinearColormap(["#004777","#72A1C6","#EFD28D","#FF7700","#A30000"],
                              vmin=1200, vmax=3000, caption="Median Rent ($)")
    cmap.add_to(m)

    for _, row in stats.iterrows():
        fips = row[county_col]
        if fips in county_coords:
            lat, lon = county_coords[fips]
            name = county_names.get(fips, fips)
            color = cmap(row["med_rent"]) if pd.notna(row["med_rent"]) else "#999"
            popup = f"<div style='font-family:Arial;width:200px'><h4 style='color:#004777;margin:0 0 8px'>{name} County</h4><b>Median Rent:</b> ${row['med_rent']:,.0f}<br><b>Median Income:</b> ${row['med_income']:,.0f}<br><b>Population:</b> {row['population']:,.0f}<br><b>Tracts:</b> {row['tracts']}</div>"
            folium.CircleMarker([lat,lon], radius=max(8, row["population"]/100000),
                                popup=folium.Popup(popup, max_width=250),
                                color=color, fill=True, fill_color=color,
                                fill_opacity=0.7, weight=2).add_to(m)

    map_path = os.path.join(OUT, "rent_map.html")
    m.save(map_path)
    print(f"  OK  rent_map.html ({os.path.getsize(map_path)//1024} KB)")
except Exception as e:
    print(f"  SKIP rent_map: {e}")

print("\n" + "="*50)
print("All charts saved to", OUT)
print("="*50)
