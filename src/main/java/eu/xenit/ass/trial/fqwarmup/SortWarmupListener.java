package eu.xenit.ass.trial.fqwarmup;

import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import org.apache.lucene.search.Query;
import org.apache.solr.common.params.CommonParams;
import org.apache.solr.common.util.ContentStreamBase.ByteArrayStream;
import org.apache.solr.common.util.NamedList;
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
 * Instances of this listener support using configured base queries and sort field definitions to pre-warm the field caches of a new SOLR
 * searcher instance with regards to sorting-related index fields.
 *
 * @author Axel Faust
 */
public class SortWarmupListener extends AbstractSolrEventListener
{

    private static final Logger LOGGER = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

    public SortWarmupListener(final SolrCore core)
    {
        super(core);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void newSearcher(final SolrIndexSearcher newSearcher, final SolrIndexSearcher currentSearcher)
    {
        final List<String> baseQueries;
        final List<String> sorts;

        final Object baseQueriesCandidate = this.getArgs().get("baseQueries");
        final Object baseQueryCandidate = this.getArgs().get("baseQuery");

        final Object sortsCandidate = this.getArgs().get("sorts");
        final Object sortCandidate = this.getArgs().get("sort");

        if (baseQueriesCandidate instanceof Collection<?> && !((Collection<?>) baseQueriesCandidate).isEmpty())
        {
            baseQueries = ((Collection<?>) baseQueriesCandidate).stream().map(String::valueOf).collect(Collectors.toList());
        }
        else if (baseQueryCandidate instanceof String && !((String) baseQueriesCandidate).trim().isEmpty())
        {
            baseQueries = Collections.singletonList((String) baseQueriesCandidate);
        }
        else
        {
            LOGGER.debug("Base query has / queries have not been properly configured - defaulting to ISNODE:T");
            baseQueries = Collections.singletonList("ISNODE:T");
        }

        if (sortsCandidate instanceof Collection<?> && !((Collection<?>) sortsCandidate).isEmpty())
        {
            sorts = ((Collection<?>) sortsCandidate).stream().map(String::valueOf).collect(Collectors.toList());
        }
        else if (sortCandidate instanceof String && !((String) sortCandidate).trim().isEmpty())
        {
            sorts = Collections.singletonList((String) sortCandidate);
        }
        else
        {
            LOGGER.debug(
                    "Sort field(s) has / have not been properly configured - defaulting to ascending/descending sorts on cm:name, cm:created and cm:modified");
            sorts = Arrays.asList("@cm:name asc", "@cm:name desc", "@cm:created asc", "@cm:created desc", "@cm:modified asc",
                    "@cm:modified desc");
        }

        final String baseRequestBody = (String) this.getArgs().get("baseRequestBody");
        Integer concurrentThreads = (Integer) this.getArgs().get("concurrentThreads");
        final Integer concurrentTimeout = (Integer) this.getArgs().get("concurrentTimeout");

        if (concurrentThreads == null || concurrentThreads.intValue() <= 0)
        {
            LOGGER.debug("Number of concurrent threads has not been properly configured - defaulting to 1");
            concurrentThreads = Integer.valueOf(1);
        }

        LOGGER.debug("Starting sort warmup");
        try
        {
            if (concurrentThreads.intValue() > 1)
            {
                final ExecutorService threadPool = Executors.newFixedThreadPool(concurrentThreads.intValue());
                try
                {
                    // we multiplex all base queries with all sort fields
                    baseQueries.forEach(q -> {
                        sorts.forEach(s -> threadPool.submit(() -> this.performQuery(newSearcher, baseRequestBody, q, s)));
                    });
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
                // we multiplex all base queries with all sort fields
                baseQueries.forEach(q -> {
                    sorts.forEach(s -> this.performQuery(newSearcher, baseRequestBody, q, s));
                });
            }
        }
        finally
        {
            LOGGER.debug("Completed sort warmup");
        }
    }

    protected void performQuery(final SolrIndexSearcher searcher, final String baseRequestBody, final String baseQuery, final String sort)
    {
        final SolrQueryRequest req = new LocalSolrQueryRequest(this.getCore(),
                new NamedList<>(Collections.singletonMap(CommonParams.SORT, sort)))
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

            searcher.getDocList(baseLuceneQuery, Collections.<Query> emptyList(), parser.getSortSpec(false).getSort(), 0, 1, 0);
        }
        catch (final SyntaxError serr)
        {
            LOGGER.warn("Failed to warmup sort due to syntax error", serr);
        }
        catch (final IOException ioex)
        {
            LOGGER.warn("Failed to warmup sort due to IO error", ioex);
        }
    }
}
