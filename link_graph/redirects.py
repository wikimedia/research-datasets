#%%

def article_redirects(snapshot, wiki_id=None):
    
    wikipedia = (spark
        ## select table
        .read.table('wmf.mediawiki_wikitext_current')
        ## select wiki project
        .where(F.col('snapshot') == snapshot )
        ## main namespace
        .where(F.col('page_namespace') == 0 )
        ## no redirect-pages
        .where(F.col('revision_text').isNotNull())
        .where(F.length(F.col('revision_text'))>0)
    )

    if wiki_id is not None:
        wikipedia = (wikipedia
            .where( F.col('wiki_db') == wiki_id ))

    # TODO
    # to create a mapping between redirected page_ids, we 
    # need to join `wikipedia` with itself since the the data
    # only contains the redirected page title, not the page_id
    # 
    # from_wiki_db, from_title, from_id, to_wiki_db, to_title, to_id]
    # only_redirects = (wikipedia
    #     .where(F.col('page_redirect_title')!='')
    # )
    # redirects = wikipedia.join(only_redirects,....)
    
    # 
    redirects = (wikipedia
        .where(F.col('page_redirect_title')!='')
        .select(
            F.col('wiki_db'),
            F.col('page_title').alias('title_from'),
            F.col('page_redirect_title').alias('title_to'))
        .distinct()
    )

    return redirects
