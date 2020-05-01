# Alfresco Search Services Filter Query and Sort warmup listener

This project creates a docker image for Alfresco Search Services with possibilities to load custom configuration for warming up:
- Filter query
- Sort caches

Image: `hub.xenit.eu/alfresco-enterprise/alfresco-solr6-fqwarmup`

The image is based on [https://github.com/xenit-eu/docker-solr](https://github.com/xenit-eu/docker-solr) and inherits all features from that image.

A custom template, based on the rerank template is added to the image:
- rerank-fqwarmup

To use this template, you can select it using an environment variable:
`TEMPLATE=rerank-fqwarmup`

Custom filter queries can be added by creating a properties file:
```$properties
{!afts}=hello:funny:"true" OR =hello:funny:"false" OR NOT ASPECT:"hello:isFunny"
{!afts}=hello:funny:"false" OR NOT ASPECT:"hello:isFunny"
{!afts}=hello:sad:"true" OR =hello:sad:"false" OR NOT ASPECT:"hello:isSad"
```

This file should be mounted in the image on path `/opt/alfresco-search-services/filterQueryList`