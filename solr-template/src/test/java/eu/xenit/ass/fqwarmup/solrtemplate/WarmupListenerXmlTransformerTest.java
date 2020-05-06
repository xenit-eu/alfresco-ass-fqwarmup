package eu.xenit.ass.fqwarmup.solrtemplate;

import eu.xenit.ass.trial.fqwarmup.FilterQueryWarmupListener;
import eu.xenit.ass.trial.fqwarmup.SortWarmupListener;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.NodeList;

class WarmupListenerXmlTransformerTest {

    @Test
    public void testTransform() throws Exception {
        WarmupListenerXmlTransformer warmupListenerXmlTransformer = new WarmupListenerXmlTransformer();
        String xmlIn =
                "<?xml version=\"1.0\" encoding=\"UTF-8\" ?>"
                        + "<config>"
                        + "<query><bla>jff</bla><!-- this is a comment -->"
                        + "</query>"
                        + "</config>";
        InputStream in = new ByteArrayInputStream(xmlIn.getBytes());
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        warmupListenerXmlTransformer.transform(in, out);

        byte[] bytes = out.toByteArray();

        System.out.println(new String(bytes));

        DocumentBuilderFactory dbFactory = DocumentBuilderFactory.newInstance();
        DocumentBuilder dBuilder;

        dBuilder = dbFactory.newDocumentBuilder();
        Document doc = dBuilder.parse(new ByteArrayInputStream(bytes));
        doc.getDocumentElement().normalize();

        Element config = doc.getDocumentElement();
        Element query = (Element) config.getElementsByTagName("query").item(0);

        NodeList elements = query.getElementsByTagName("listener");
        List<Element> elementList = new ArrayList<>();
        for (int i = 0; i < elements.getLength(); i++) {
            elementList.add((Element) elements.item(i));
        }

        checkListener(elementList, "firstSearcher", FilterQueryWarmupListener.class.getCanonicalName());
        checkListener(elementList, "newSearcher", FilterQueryWarmupListener.class.getCanonicalName());

        checkListener(elementList, "firstSearcher", SortWarmupListener.class.getCanonicalName());
        checkListener(elementList, "newSearcher", SortWarmupListener.class.getCanonicalName());


    }

    private void checkListener(List<Element> elementList, String event, String canonicalName) {
        Assertions.assertTrue(elementList.stream().anyMatch(element -> {
            return element.getAttribute("event").equals(event) &&
                    element.getAttribute("class").equals(canonicalName);
        }));
    }

}