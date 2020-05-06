package eu.xenit.ass.fqwarmup.solrtemplate;

import eu.xenit.ass.trial.fqwarmup.FilterQueryWarmupListener;
import eu.xenit.ass.trial.fqwarmup.SortWarmupListener;
import java.io.InputStream;
import java.io.OutputStream;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.transform.OutputKeys;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.dom.DOMSource;
import javax.xml.transform.stream.StreamResult;
import org.w3c.dom.Document;
import org.w3c.dom.Element;

public class WarmupListenerXmlTransformer {

    public void transform(InputStream in, OutputStream out) throws Exception {
        DocumentBuilderFactory dbFactory = DocumentBuilderFactory.newInstance();
        DocumentBuilder dBuilder;

        dBuilder = dbFactory.newDocumentBuilder();
        Document doc = dBuilder.parse(in);
        doc.getDocumentElement().normalize();

        Element config = doc.getDocumentElement();
        Element query = (Element) config.getElementsByTagName("query").item(0);

        addFilterQueryWarmupListener(doc, query, "newSearcher");
        addFilterQueryWarmupListener(doc, query, "firstSearcher");

        addSortWarmupListener(doc, query, "newSearcher");
        addSortWarmupListener(doc, query, "firstSearcher");

        TransformerFactory transformerFactory = TransformerFactory.newInstance();
        Transformer transformer = transformerFactory.newTransformer();
        DOMSource source = new DOMSource(doc);
        StreamResult result = new StreamResult(out);
        transformer.setOutputProperty(OutputKeys.INDENT, "yes");
        transformer.transform(source, result);
    }

    private static void addFilterQueryWarmupListener(Document doc, Element query, String event) {
        Element filterQueryWarmupListener = getListenerElement(doc, event,
                FilterQueryWarmupListener.class.getCanonicalName());

        addStringElement(doc, filterQueryWarmupListener, "baseQuery", "{!afts}ISNODE:T");
        addStringElement(doc, filterQueryWarmupListener, "baseRequestBody",
                "{\"tenants\":[\"\"],\"locales\":[\"en_\"],\"defaultNamespace\":\"http://www.alfresco.org/model/content/1.0\",\"textAttributes\":[],\"defaultFTSOperator\":\"OR\",\"defaultFTSFieldOperator\":\"OR\",\"templates\":[],\"allAttributes\":[],\"queryConsistency\":\"EVENTUAL\"}");
        addStringElement(doc, filterQueryWarmupListener, "queryListFile",
                "/opt/alfresco-search-services/filterQueryList");
        addIntElement(doc, filterQueryWarmupListener, "concurrentThreads", 8);
        addIntElement(doc, filterQueryWarmupListener, "concurrentTimeout", 300000);
        query.appendChild(filterQueryWarmupListener);
    }

    private static void addSortWarmupListener(Document doc, Element query, String event) {
        Element sortWarmupListener = getListenerElement(doc, event, SortWarmupListener.class.getCanonicalName());
        addStringArray(doc, sortWarmupListener, "baseQueries", "{!afts}ISNODE:T", "{!afts}TYPE:\"cm:folder\"",
                "{!afts}TYPE:\"cm:content\"");
        addStringArray(doc, sortWarmupListener, "sorts", "@cm:name asc", "@cm:name asc");
        addStringElement(doc, sortWarmupListener, "baseRequestBody",
                "{\"tenants\":[\"\"],\"locales\":[\"en_\"],\"defaultNamespace\":\"http://www.alfresco.org/model/content/1.0\",\"textAttributes\":[],\"defaultFTSOperator\":\"OR\",\"defaultFTSFieldOperator\":\"OR\",\"templates\":[],\"allAttributes\":[],\"queryConsistency\":\"EVENTUAL\"}");
        addIntElement(doc, sortWarmupListener, "concurrentThreads", 8);
        addIntElement(doc, sortWarmupListener, "concurrentTimeout", 300000);
        query.appendChild(sortWarmupListener);
    }

    private static void addStringArray(Document doc, Element parent, String name, String... values) {
        Element array = doc.createElement("arr");
        array.setAttribute("name", name);
        for (String value : values) {
            Element string = doc.createElement("str");
            string.setTextContent(value);
            array.appendChild(string);
        }
        parent.appendChild(array);
    }

    private static Element getListenerElement(Document doc, String event, String canonicalName) {
        Element searcherSolrWarmupListnener = doc.createElement("listener");
        searcherSolrWarmupListnener.setAttribute("event", event);
        searcherSolrWarmupListnener.setAttribute("class", canonicalName);
        return searcherSolrWarmupListnener;
    }

    private static void addStringElement(Document doc, Element parent, String name, String textContent) {
        addElement(doc, parent, "str", name, textContent);
    }

    private static void addIntElement(Document doc, Element parent, String name, int value) {
        addElement(doc, parent, "int", name, Integer.toString(value));
    }

    private static void addElement(Document doc, Element parent, String element, String name, String textContent) {
        Element baseQuery = doc.createElement(element);
        baseQuery.setAttribute("name", name);
        baseQuery.setTextContent(textContent);
        parent.appendChild(baseQuery);
    }

}
