#%%
# !pip install plotly
# !pip install iso3166
# !pip install -U kaleido
# !pip install pillow

## !pip install psutil # might not be needed?
#%%

from io import StringIO, BytesIO
import iso3166 
from PIL import Image
import plotly.graph_objects as go

def append_alpha3(df):
    # some country names don't match between
    # the names wmf uses and iso3166 names.
    remap_countries = {
        'United States' : 'UNITED STATES OF AMERICA',
        'Ivory Coast' : "CÔTE D'IVOIRE",
        'Russia' : 'RUSSIAN FEDERATION',
        'Iran' : 'IRAN, ISLAMIC REPUBLIC OF',
        'Unknown' : 'Unknown',
        'Republic of Lithuania' : 'LITHUANIA',
        'Republic of Moldova' : 'MOLDOVA, REPUBLIC OF',
        'Tanzania' : 'TANZANIA, UNITED REPUBLIC OF',
        'South Korea' : 'KOREA, REPUBLIC OF',
        'Bolivia' : 'BOLIVIA, PLURINATIONAL STATE OF',
        'DR Congo' : 'CONGO, DEMOCRATIC REPUBLIC OF THE',
        'United Kingdom' : 'UNITED KINGDOM OF GREAT BRITAIN AND NORTHERN IRELAND',
        'Hashemite Kingdom of Jordan' : 'JORDAN'}

    notfound = set()
    def isoA3(c):
        if c in remap_countries:
            c = remap_countries[c]  
        x = iso3166.countries_by_apolitical_name.get(c.upper())
        if x is not None:
            return x.alpha3
        x = iso3166.countries_by_name.get(c.upper())
        if x is not None:
            return x.alpha3
        notfound.add(c)

    df['A3'] = df['country'].apply(isoA3)
    print(f"{len(notfound)} country codes not found: {notfound}")


def animate_choropleth(
    df,
    frame_unit,
    show=False,
    value_range=(None, None),
    title=False,
    frame_title_fn=None,
    write_gif=None,
    duration=400):
    """
    animate a pandas dataframe on a chloropeth map. 

    df: a dataframe with required columns `country`, `{frame_unit}` and `value`
    frame_unit: for each value of this column, there should be a row for each country
    show: if true, calls fig.show(renderer="notebook")
    value_range: min/max tuple for the range of values, ie useful for a fixed colorbar
    title: for a fixed title across all frames. also see frame_title_fn
    frame_title_fn: define a custom title for each frame
    write_gif: if not None, generates a gif file for the provided path
    duration: duration of each frame

    In ascending order, for each `frame_unit` value a frame is generated for the animation
    """
    
    append_alpha3(df)
    first = df.loc[(df[frame_unit]==df[frame_unit].min(axis=0)) & (df['A3'].notnull()),:]

    zmin, zmax = value_range    
    choropleth_frames = []
    for fu in df[frame_unit].unique():
        frame_data = df.loc[(df[frame_unit]==fu) & (df['A3'].notnull()),:]
        frame_title = title if frame_title_fn is None else frame_title_fn(fu)
        cp = go.Choropleth(
            locations = frame_data['A3'],
            z = frame_data['value'],
            zmin = zmin,
            zmax = zmax,
            text = frame_data['country'],
            colorscale = 'Blues',
            autocolorscale=False,
            marker_line_color='darkgray',
            marker_line_width=0.5,
        )
        l = go.Layout(
            title_text=frame_title,
            geo=dict(
                showframe=False,
                showcoastlines=False,
                projection_type='equirectangular'
            ))

        choropleth_frames.append({"data":[cp],"layout":l})

    if write_gif is not None:
        print(f'generating gif to {write_gif}')
        images = []
        for i,frame_dict in enumerate(choropleth_frames):
            im_bytes = go.Figure(frame_dict).to_image(format="png", engine="kaleido")
            images.append(Image.open(BytesIO(im_bytes)))
                
        images[0].save(write_gif, save_all=True, append_images=images[1:], duration=duration, loop=0)


    fig = go.Figure(
        data=go.Choropleth(
            locations = first['A3'],
            z = first['value'],     
            zmin = zmin,
            zmax = zmax,                   
            text = first['country'],
            colorscale = 'Blues',
            autocolorscale=False,            
            marker_line_color='darkgray',
            marker_line_width=0.5,            
        ),
        layout=go.Layout(
            title_text=f'{title} {fu}',
            geo=dict(
                showframe=False,
                showcoastlines=False,
                projection_type='equirectangular'
            ),            
            updatemenus=[dict(
                type="buttons",
                buttons=[dict(label="Play",
                            method="animate",
                            args=[None])])]),
        frames=[go.Frame(fd) for fd in choropleth_frames])

    if show: 
        fig.show(renderer="notebook")
    
    return fig

# %%
