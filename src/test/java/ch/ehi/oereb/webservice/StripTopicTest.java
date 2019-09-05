package ch.ehi.oereb.webservice;

import static org.junit.Assert.*;

import java.net.MalformedURLException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.util.List;

import org.junit.Test;
import org.springframework.util.MultiValueMap;
import org.springframework.web.util.UriComponentsBuilder;

public class StripTopicTest  {
    @Test
    public void subTopic()
    {
        assertEquals("Nutzungsplanung",OerebController.stripSubTopic("ch.so.Nutzungsplanung.NutzungsplanungGrundnutzung"));
    }
    @Test
    public void cantonalTopic()
    {
        assertEquals("ch.so.Einzelschutz",OerebController.stripSubTopic("ch.so.Einzelschutz"));
    }
    @Test
    public void federalTopic()
    {
        assertEquals("Nutzungsplanung",OerebController.stripSubTopic("Nutzungsplanung"));
    }
}
