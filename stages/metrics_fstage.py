from processorpipeline import AbstractUsageStatsPipelineStage, UsageStatsData
from configcontext import ConfigurationContext


class MetricsFilterStage(AbstractUsageStatsPipelineStage):
    def __init__(self, configContext: ConfigurationContext):
        super().__init__(configContext)

        # get the actions from the configuration
        self.actions = configContext.getActions()
        self.actions_id = configContext.getActionsId()

        # get labels
        self.ACTION_TYPE_LABEL = configContext.getLabel('ACTION_TYPE')
        self.OAI_IDENTIFIER_LABEL = configContext.getLabel('OAI_IDENTIFIER')
        self.ID_VISIT_LABEL = configContext.getLabel('ID_VISIT')
        self.COUNTRY_LABEL = configContext.getLabel('COUNTRY')

    
    def run(self, data: UsageStatsData) -> UsageStatsData:

        actions = []    

        # initialize the views, outlinks and downloads columns
        for action, action_id in zip(self.actions, self.actions_id):
            if ( action_id > 0 ):
                data.events_df[action] = 0
                data.events_df.loc[data.events_df[ self.ACTION_TYPE_LABEL ] == action_id, action] = 1
                actions.append(action) 

        ## create dataframe with the columns oai_identifier and country, where country in not null or empty string
        country_df = data.events_df[ (data.events_df[self.COUNTRY_LABEL].notnull()) & (data.events_df[self.COUNTRY_LABEL] != '') ]
        ## create a dict based on the country_df dataframe, where the key is the oai_identifier and the value is the country
        data.country_by_identifier_dict = country_df.set_index(self.OAI_IDENTIFIER_LABEL)[self.COUNTRY_LABEL].to_dict()


        # group by idvisit and oai_identifier and sum the views, outlinks and downloads columns
        data.events_df = data.events_df.groupby([self.ID_VISIT_LABEL, self.OAI_IDENTIFIER_LABEL]).agg( dict((action,'max') for action in actions )).reset_index()

        # merge the events and visits dataframes on the idvisit column
        data.events_df = data.events_df.merge(data.visits_df, on='idvisit')
        
        # create a new column called conversions that is 1 if the views and downloads columns are 1, 0 otherwise
        data.events_df['conversions'] = ( (data.events_df['views'] == 1) & ( ( (data.events_df['downloads'] == 1) | (data.events_df['outlinks'] == 1)))).astype(int)
        
        return data       


