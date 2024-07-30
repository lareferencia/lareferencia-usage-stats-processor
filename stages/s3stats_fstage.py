from processorpipeline import AbstractUsageStatsPipelineStage, UsageStatsData
from configcontext import ConfigurationContext



class S3StatsOutputStage(AbstractUsageStatsPipelineStage):
    def __init__(self, configContext: ConfigurationContext):
        super().__init__(configContext)
        
    
    def run(self, data: UsageStatsData) -> UsageStatsData:

        idsite = self.getCtx().getArg('site')
        year = self.getCtx().getArg('year')
        month = self.getCtx().getArg('month')

                        
        # print events size
        print("Events size: %d" % data.events_df.size)

        ## create a new datafran agregating events by year and month
        data.events_df['year'] = data.events_df['server_time'].dt.year
        data.events_df['month'] = data.events_df['server_time'].dt.month
        data.events_df['day'] = data.events_df['server_time'].dt.day

        data.events_df = data.events_df.groupby(['year', 'month']).size().reset_index(name='count')

        # make a histogram of the events by day
        # data.events_df.plot(x='day', y='count', kind='bar', title="Events by day")

        # save the plot in a file with idiste, year
        fig = data.events_df.plot(x='month', y='count', kind='bar', title="Site %s - Events by month - year %s " % (idsite,year) ).get_figure()
        fig.savefig('report/events_%s_%s.png' % (idsite, year) )
                
        return data
     
 
        