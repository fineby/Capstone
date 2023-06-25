import numpy as np
import matplotlib.pyplot as plt
import plotly.graph_objects as go
import seaborn as sns
import folium
from folium.features import DivIcon
import webbrowser
import os, time

#3.2 Used Folium with external GEO data from file to generate Chrolopleth map with automatic linspace to 3 category.
# For output generated HTML file. It's saved to the HDD and loaded to the default browser.
def usa_map_display(pd_gr2):
    us_geo = str(os.path.sys.path[0])+"\Assets\geo.json"
    state_scale = np.linspace(pd_gr2['Number of customers'].min(), pd_gr2['Number of customers'].max(), 4, dtype=int)
    state_scale = state_scale.tolist()
    state_scale[-1] = state_scale[-1] + 1
    states_map = folium.Map(location=[39.833333,-99.890833], zoom_start=4, zoom_control=False, \
    scrollWheelZoom=False, dragging=False)
    states_map.choropleth(geo_data=us_geo, data=pd_gr2, columns=['Customer state', 'Number of customers'], \
    key_on='feature.properties.NAME', threshold_scale=state_scale, fill_color='YlOrRd', fill_opacity=0.7,  line_opacity=0.2, \
    nan_fill_color="lightgray", reset=True)
    folium.map.Marker((52.08,-110.02), icon=DivIcon( icon_size=(280,50), icon_anchor=(5,14),
    html=f'<div style="font-size: 14pt">%s</div>' % str('Number of customers by states with bins (LOW, MIDDLE, HIGH)'))).add_to(states_map)
    folium.map.Marker((42.58,-76.52), icon=DivIcon( icon_size=(100,50), icon_anchor=(5,14),
    html=f'<div style="font-size: 10pt">%s</div>' % str('NY 96'))).add_to(states_map)
    folium.map.Marker((40.58,-78.52), icon=DivIcon( icon_size=(100,50), icon_anchor=(5,14),
    html=f'<div style="font-size: 10pt">%s</div>' % str('PA 72'))).add_to(states_map)
    folium.map.Marker((32.58,-83.82), icon=DivIcon( icon_size=(30,50), icon_anchor=(5,14),
    html=f'<div style="font-size: 10pt">%s</div>' % str('GA 73'))).add_to(states_map)
    states_map.save(str(os.path.sys.path[0])+"\map.html")
    webbrowser.open(str(os.path.sys.path[0])+"\map.html")
    time.sleep(3)
    if os.path.isfile("map.html"): os.remove("map.html")

def gr_group1(pd_gr1, pd_gr3, pd_gr2):
    os.system('cls')
    sns.set(rc={'axes.facecolor':'lightgrey', 'figure.facecolor':'lightgrey'})
    #3.1 Mathplotlib histogram with automatic binning by Numpy. Categorical and numerical edges displaing 
    # at the chart.
    count_bin, bin_edges = np.histogram(pd_gr1['Transaction rate'], 3)
    pd_gr1['Transaction rate'].plot(kind='hist', xticks=bin_edges, color=['#003f5c'])
    plt.title('Number of transactions by category')
    plt.ylabel('Category frequency')
    plt.xlabel('Bins')
    plt.yticks(np.arange(0, 3, step=1))
    plt.xticks([(bin_edges[0]+bin_edges[1])/2, (bin_edges[1]+bin_edges[2])/2, (bin_edges[2]+bin_edges[3])/2], \
    [f'Low ({str(round(bin_edges[0]))+"-"+str(round(bin_edges[1]))})', \
    f'Middle ({str(round(bin_edges[1]))+"-"+str(round(bin_edges[2]))})', \
    f'High ({str(round(bin_edges[2]))+"-"+str(round(bin_edges[3]))})'])
    plt.text(bin_edges[3]-23, 0.4, f"{pd_gr1.iloc[0,0]}", ha='left', va='bottom',fontsize=15, rotation=90, color='white')
    plt.show()
    os.system('cls')

    # 3.3  Mathplotlib line chart with annotation.
    plt.rcParams["figure.figsize"] = (8,5)
    pd_gr3.plot(title='Sum of all transactions for top 10 customers',ylabel='Sum',legend='',marker='o',ms=10, linewidth=3.0)
    plt.xlabel('Customer')
    plt.xticks(np.arange(0,10,step=1),pd_gr3.index.values)
    plt.grid(linestyle='--')
    plt.annotate('', xy=(9, 5633.07), xytext=(2, 5500), xycoords='data', arrowprops=dict(arrowstyle='->', \
    connectionstyle='arc3', color='red', lw=2))
    plt.annotate(f'Customer with the highest transaction amount {round(pd_gr3.iloc[9,0],2)}', xy=(2, 5500), \
    rotation=10, va='bottom', ha='left', fontsize=10)
    plt.tight_layout()
    plt.show()
    plt.rcParams["figure.figsize"] = plt.rcParamsDefault["figure.figsize"]
    i1=input('Press 1 to display "Number of customers by states" or any other input for Main Menu: ')
    os.system('cls')
    if i1=='1': usa_map_display(pd_gr2)


def h_trans_display(pd_gr7):
    #5.4 Plotly interactive scatter plot chart with plot for the branch with highest dollar value.
    fig=go.Figure()
    fig.update_xaxes(categoryorder='array', categoryarray= pd_gr7['Branch'][1:])
    fig.add_trace(go.Scatter(x=pd_gr7['Branch'][1:], y=pd_gr7['Healthcare transactions'][1:], mode='markers', marker=dict(color='blue')))
    fig.add_trace(go.Scatter(x=pd_gr7['Branch'][:1], y=pd_gr7['Healthcare transactions'][:1], mode='markers', marker=dict(color='red')))
    fig.update_layout({'paper_bgcolor': 'lightgrey'},
    title={'text': 'Branches processed healthcare transactions','y':0.95,'x':0.5,'xanchor': 'center',
    'yanchor': 'top'},xaxis_title="Branch number", yaxis_title="Total dollar value",showlegend=False)
    fig.update_xaxes(range=[0, 193], nticks=50, gridcolor='black', griddash='dot')
    fig.update_yaxes(range=[1620, 4500])
    fig.add_annotation(x=pd_gr7['Branch'][0], y=pd_gr7['Healthcare transactions'][0],
    text=f"Branch {pd_gr7['Branch'][0]} with highest total dollar value {round(pd_gr7['Healthcare transactions'][0],2)}", \
    showarrow=True, arrowhead=1,)
    fig.show()

def gr_group2(pd_gr4, pd_gr5, pd_gr6, pd_gr7):
    os.system('cls')
    sns.set(rc={'axes.facecolor':'lightgrey', 'figure.facecolor':'lightgrey'})
    #5.1 Mathplotlib pie chart.
    ax = plt.axes()
    ax.pie(pd_gr4["Value"], labels=pd_gr4["Category"], autopct='%1.2f%%', labeldistance=.28, \
    colors=['#ffa600','#003f5c'], shadow=True, explode=[0,0.08], textprops={'fontsize': 10, 'color': 'white'})
    ax.set_title('Percentage of applications for self-employed applicants', fontsize=14)
    plt.tight_layout()
    plt.show()

    #5.2 Mathplotlib bars multiple chart.
    X = ['Rejection','Approving']
    Y = pd_gr5.iloc[:2,1]
    Z = pd_gr5.iloc[2:,1]
    X_axis = np.arange(len(X))
    plt.bar(X_axis - 0.2, Y, 0.4, label = 'Male', color=['#003f5c'])
    plt.bar(X_axis + 0.2, Z, 0.4, label = 'Female', color=['#bc5090'])
    plt.xticks(X_axis, X)
    plt.xlabel("Application")
    plt.ylabel("Percentage")
    plt.title("Married applicants")
    plt.legend()
    plt.text(-0.35, 7, f"{pd_gr5.iloc[0,1]/100:.2%}", ha='left', va='bottom', fontsize=15, color='white')
    plt.rcParams['axes.facecolor'] = 'lightgrey'
    plt.show()

    #5.3 Seaborn horizontal bars chart.
    f, ax = plt.subplots() 
    sns.barplot(x = 'Transactions', y = 'Month', data = pd_gr6, color = '#ffa600', edgecolor = 'w'). \
    set_title('The top three months with the largest transaction data')
    ax.set_xlim(200000,203000)
    ax.bar_label(ax.containers[0])
    plt.show()

    i1=input('Press 1 to display "Branches processed healthcare transactions" or any other input for Main Menu: ')
    os.system('cls')
    if i1=='1': h_trans_display(pd_gr7)