package eu.xenit.ass.trial.fqwarmup;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.lang.invoke.MethodHandles;
import java.nio.charset.StandardCharsets;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.apache.lucene.search.Query;
import org.apache.solr.common.util.ContentStreamBase.ByteArrayStream;
import org.apache.solr.core.AbstractSolrEventListener;
import org.apache.solr.core.SolrCore;
import org.apache.solr.request.LocalSolrQueryRequest;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.search.QParser;
import org.apache.solr.search.SolrIndexSearcher;
import org.apache.solr.search.SyntaxError;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Instances of this listener support using an externally configured list of filter queries to pre-warm the filter query cache of a new SOLR
 * searcher instance.
 *
 * @author Axel Faust
 */
public class FilterQueryWarmupListener extends AbstractSolrEventListener
{

    private static final Logger LOGGER = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

    public FilterQueryWarmupListener(final SolrCore core)
    {
        super(core);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void newSearcher(final SolrIndexSearcher newSearcher, final SolrIndexSearcher currentSearcher)
    {
        String baseQuery = (String) this.getArgs().get("baseQuery");
        final String baseRequestBody = (String) this.getArgs().get("baseRequestBody");
        Integer concurrentThreads = (Integer) this.getArgs().get("concurrentThreads");
        final Integer concurrentTimeout = (Integer) this.getArgs().get("concurrentTimeout");

        if (baseQuery == null || baseQuery.trim().isEmpty())
        {
            LOGGER.debug("Base query has not been properly configured - defaulting to ISNODE:T");
            baseQuery = "ISNODE:T";
        }

        if (concurrentThreads == null || concurrentThreads.intValue() <= 0)
        {
            LOGGER.debug("Number of concurrent threads has not been properly configured - defaulting to 1");
            concurrentThreads = Integer.valueOf(1);
        }

        final String filePath = (String) this.getArgs().get("queryListFile");
        final File file = Paths.get(filePath).toFile();
        if (file.exists() && file.isFile())
        {
            final Set<String> filterQueries = new LinkedHashSet<>();

            try (BufferedReader br = new BufferedReader(new InputStreamReader(new FileInputStream(file), StandardCharsets.UTF_8)))
            {
                String line;
                while ((line = br.readLine()) != null)
                {
                    if (!line.trim().isEmpty())
                    {
                        filterQueries.add(line.trim());
                    }
                    else
                    {
                        LOGGER.trace("Skipping empty line");
                    }
                }
                LOGGER.debug("Read {} filter queries list from {}", filterQueries.size(), file);
            }
            catch (final IOException ioex)
            {
                LOGGER.warn("Error handling query list file", ioex);
            }

            if (!filterQueries.isEmpty())
            {
                LOGGER.debug("Starting filter query warmup");
                try
                {
                    if (concurrentThreads.intValue() > 1)
                    {
                        final ExecutorService threadPool = Executors.newFixedThreadPool(concurrentThreads.intValue());
                        try
                        {
                            final String baseQueryF = baseQuery;
                            filterQueries.forEach(
                                    fq -> threadPool.submit(() -> this.performQuery(newSearcher, baseRequestBody, baseQueryF, fq)));
                        }
                        finally
                        {
                            threadPool.shutdown();

                            final long timeout = concurrentTimeout == null || concurrentTimeout.intValue() < 0 ? 10000
                                    : concurrentTimeout.longValue();
                            LOGGER.debug("Waiting for termination of concurrent warmup threads");
                            try
                            {
                                if (threadPool.awaitTermination(timeout == 0 ? Long.MAX_VALUE : timeout, TimeUnit.MILLISECONDS))
                                {
                                    LOGGER.debug("All concurrent warmup threads completed in the allotted time");
                                }
                                else
                                {
                                    final List<Runnable> incompleteTasks = threadPool.shutdownNow();
                                    LOGGER.warn(
                                            "Concurrent filter query warmup threads did not complete in the allotted time, leaving {} warmup tasks uncompleted",
                                            incompleteTasks.size());
                                }
                            }
                            catch (final InterruptedException iex)
                            {
                                LOGGER.warn("Thread was interrupted waiting for concurrent filter query warmup threads");
                                // reset the interrupted flag
                                Thread.currentThread().interrupt();
                            }
                        }
                    }
                    else
                    {
                        this.performQuery(newSearcher, baseRequestBody, baseQuery, filterQueries.toArray(new String[0]));
                    }
                }
                finally
                {
                    LOGGER.debug("Completed filter query warmup");
                }
            }
            else
            {
                LOGGER.debug("Skipping filter query warmup as no queries could be read from {}", file);
            }
        }
        else
        {
            LOGGER.warn("Not running filter query warmup as query list file parameter value {} does not point to a valid file", filePath);
        }
    }

    protected void performQuery(final SolrIndexSearcher searcher, final String baseRequestBody, final String baseQuery,
            final String... filterQueries)
    {
        final SolrQueryRequest req = new LocalSolrQueryRequest(this.getCore(), Collections.emptyMap())
        {

            {
                this.streams = Arrays.asList(new ByteArrayStream(baseRequestBody.getBytes(StandardCharsets.UTF_8), baseRequestBody));
            }

            /**
             * {@inheritDoc}
             */
            @Override
            public SolrIndexSearcher getSearcher()
            {
                return searcher;
            }

            /**
             * {@inheritDoc}
             */
            @Override
            public void close()
            {
            }
        };

        try
        {
            final QParser parser = QParser.getParser(baseQuery, req);
            final Query baseLuceneQuery = parser.getQuery();
            // brute force warmup in one massive query
            final List<Query> luceneFilterQueries = new ArrayList<>();

            for (final String filterQuery : filterQueries)
            {
                luceneFilterQueries.add(QParser.getParser(filterQuery, req).getQuery());
            }

            searcher.getDocList(baseLuceneQuery, luceneFilterQueries, null, 0, 1, 0);
        }
        catch (final SyntaxError serr)
        {
            LOGGER.warn("Failed to warmup filter query due to syntax error", serr);
        }
        catch (final IOException ioex)
        {
            LOGGER.warn("Failed to warmup filter query due to IO error", ioex);
        }
    }
}
