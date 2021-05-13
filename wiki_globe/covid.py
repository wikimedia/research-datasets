#%%
import pandas as pd

# downloaded from https://figshare.com/articles/dataset/COVID-19_Pandemic_Wikipedia_Readership/14548032?file=27917895
covid_pageviews_file = "~/Downloads/covid_pageviews.tsv"
cpv = pd.read_csv(covid_pageviews_file, sep='\t', names=['year', 'week','country','project','page','views'])
cpv['is_covid'] = cpv['page'].apply(lambda t: t != 'control')
by_country = cpv[cpv['is_covid']].drop(['year','project'],axis=1).groupby(['week','country','is_covid'],as_index=False).sum(['views'])
peak = by_country.drop(['is_covid', 'week'],axis=1).groupby('country').max('views').rename(columns={'views':'peak'})
pv = by_country.join(peak,on='country')
pv['value'] = pv['views']/pv['peak']

#%%
fig = animate_choropleth(
    pv,
    'week', 
    'Peak Covid19 Interest - Week ', 
    value_range=(0,1),
    write_gif='covid.gif',
    duration=500)

