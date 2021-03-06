#%%
# !pip install plotly
# !pip install iso3166

import plotly.graph_objects as go
import iso3166 

#%%

def append_alpha3(df):
    codes = {}
    # for _,r in pd.read_csv('http://dev.maxmind.com/static/csv/codes/iso3166.csv').iterrows():
    for _,r in pd.read_csv('iso3166.csv').iterrows():
        c = iso3166.countries_by_alpha2.get(r['A1'])
        if c is not None:
            codes[r['Anonymous Proxy']] = c.alpha3
        else:
            print(f"not found {r['Anonymous Proxy']}")

    codes['Namibia'] = 'NAM'

    notfound = set()
    def isoA3(country):
        # c = iso3166.countries_by_name.get(country.upper())
        # return c.alpha3 if c is not None else 'Whodat?'
        a3 = codes.get(country)
        if a3 is None:
            notfound.add(country)
        return codes.get(country,None)

    df['A3'] = df['country'].apply(isoA3)
    print(f"{len(notfound)} country codes not found: {notfound}")


def animate_pageviews(df, frame_unit):
    """
    animate the dataframe on a chloropeth map. 
    
    `df` needs to have columns `country`, `frame_unit` and `count`

    In ascending order, for each `frame_unit` value a frame is generated for the animation
    """
    append_alpha3(df)
    first = df.loc[(df[frame_unit]==df[frame_unit].min(axis=0)) & (df['A3'].notnull()),:]

    choropleth_frames = []
    for fu in df[frame_unit].unique():
        frame_data = df.loc[(df[frame_unit]==fu) & (df['A3'].notnull()),:]
        cp = go.Choropleth(
            locations = frame_data['A3'],
            z = frame_data['count'],
            text = frame_data['country'],
            colorscale = 'Blues',
            autocolorscale=False,
            marker_line_color='darkgray',
            marker_line_width=0.5,
            colorbar_title = 'Pageviews',
        )
        l = go.Layout(
            title_text=f'Pageviews - {fu}',
            geo=dict(
                showframe=False,
                showcoastlines=False,
                projection_type='equirectangular'
            ),
            annotations = [dict(
                x=0.55,
                y=0.1,
                xref='paper',
                yref='paper',
                text='Pageviews',
                showarrow = False
            )])

        choropleth_frames.append(go.Frame(data=[cp],layout=l))

    fig = go.Figure(
        data=go.Choropleth(
            locations = first['A3'],
            z = first['count'],
            text = first['country'],
            colorscale = 'Blues',
            autocolorscale=False,
            # reversescale=True,
            marker_line_color='darkgray',
            marker_line_width=0.5,
            # colorbar_tickprefix = '$',
            colorbar_title = 'Pagegviews'),
        layout=go.Layout(
            title_text='Pageviews',
            geo=dict(
                showframe=False,
                showcoastlines=False,
                projection_type='equirectangular'
            ),
            annotations = [dict(
                x=0.55,
                y=0.1,
                xref='paper',
                yref='paper',
                text='Pageviews',           
                showarrow = False
            )],
            updatemenus=[dict(
                type="buttons",
                buttons=[dict(label="Play",
                            method="animate",
                            args=[None])])]),
        frames=choropleth_frames)

    fig.update_layout(
        title_text='Pageviews',
        geo=dict(
            showframe=False,
            showcoastlines=False,
            projection_type='equirectangular'
        ),
        annotations = [dict(
            x=0.55,
            y=0.1,
            xref='paper',
            yref='paper',
            text='Pageviews',
            showarrow = False
        )]
    )
    
    fig.show(renderer="notebook")
