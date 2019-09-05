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

public class UrlParserTest  {
    @Test
    public void paramParser()
            throws Exception
    {
        String uriTxt="https://geo.so.ch/api/wms?SERVICE=WMS&VERSION=1.3.0&REQUEST=GetMap&FORMAT=image%2Fpng&TRANSPARENT=true&LAYERS=ch.so.agi.hintergrundkarte_farbig&STYLES=&SRS=EPSG%3A2056&CRS=EPSG%3A2056&TILED=false&DPI=96&OPACITIES=255&t=675&WIDTH=1920&HEIGHT=710&BBOX=2607051.2375,1228517.0374999999,2608067.2375,1228892.7458333333";
        final UriComponentsBuilder builder = UriComponentsBuilder.fromUriString(uriTxt);
        MultiValueMap<String, String> parameters =
                builder.build().getQueryParams();
        List<String> param1 = parameters.get("STYLES");
        List<String> param2 = parameters.get("BBOX");
        System.out.println("STYLES: " + param1.get(0));
        System.out.println("BBOX: " + param2.get(0));        
        builder.replaceQueryParam("BBOX", (Object[])null);
        builder.replaceQueryParam("XX", "YY");
        final String newUriTxt = builder.build().toUriString();
        System.out.println("new uri <"+newUriTxt+">");
        assertEquals("https://geo.so.ch/api/wms?SERVICE=WMS&VERSION=1.3.0&REQUEST=GetMap&FORMAT=image%2Fpng&TRANSPARENT=true&LAYERS=ch.so.agi.hintergrundkarte_farbig&STYLES=&SRS=EPSG%3A2056&CRS=EPSG%3A2056&TILED=false&DPI=96&OPACITIES=255&t=675&WIDTH=1920&HEIGHT=710&XX=YY",newUriTxt);
        
    }
}
