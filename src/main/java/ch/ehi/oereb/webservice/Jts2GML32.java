package ch.ehi.oereb.webservice;

import ch.ehi.oereb.schemas.gml._3_2.*;

public class Jts2GML32 {
    public MultiSurfacePropertyTypeType convertMultiSurface(com.vividsolutions.jts.geom.Geometry geometry) {
        if(geometry instanceof com.vividsolutions.jts.geom.Polygon) {
            SurfacePropertyTypeType surfaceProperty=new SurfacePropertyTypeType();
            surfaceProperty.setAbstractSurface(convertPolygon((com.vividsolutions.jts.geom.Polygon)geometry));
            SurfaceMember surfaceMember=new SurfaceMember();
            surfaceMember.setValue(surfaceProperty);
            MultiSurfaceTypeType multiSurface=new MultiSurfaceTypeType();
            multiSurface.getSurfaceMember().add(surfaceMember);
            MultiSurfacePropertyTypeType ret=new MultiSurfacePropertyTypeType();
            ret.setMultiSurface(new MultiSurface(multiSurface));
            return ret;
        }else if(geometry instanceof com.vividsolutions.jts.geom.MultiPolygon) {
            MultiSurfaceTypeType multiSurface=new MultiSurfaceTypeType();
            com.vividsolutions.jts.geom.MultiPolygon jtsMulti=(com.vividsolutions.jts.geom.MultiPolygon)geometry;
            for(int i=0;i<jtsMulti.getNumGeometries();i++) {
                com.vividsolutions.jts.geom.Polygon jtsPoly=(com.vividsolutions.jts.geom.Polygon)jtsMulti.getGeometryN(i);
                SurfacePropertyTypeType surfaceProperty=new SurfacePropertyTypeType();
                surfaceProperty.setAbstractSurface(convertPolygon(jtsPoly));
                SurfaceMember surfaceMember=new SurfaceMember();
                surfaceMember.setValue(surfaceProperty);
                multiSurface.getSurfaceMember().add(surfaceMember);
            }
            MultiSurfacePropertyTypeType ret=new MultiSurfacePropertyTypeType();
            ret.setMultiSurface(new MultiSurface(multiSurface));
            return ret;
        }
        throw new IllegalArgumentException("unexpected geometry type");
    }
    public Polygon convertPolygon(com.vividsolutions.jts.geom.Polygon geometry) {
        
        com.vividsolutions.jts.geom.LineString jtsLineString=geometry.getExteriorRing();
        LinearRing ring = createLinearRing(jtsLineString);
        AbstractRingPropertyTypeType exteriorRingProperty = new AbstractRingPropertyTypeType();
        exteriorRingProperty.setAbstractRing(ring);
        
        PolygonTypeType polygon=new PolygonTypeType();
        polygon.setExterior(new Exterior(exteriorRingProperty));
        return new Polygon(polygon);
    }
    public LinearRing createLinearRing(com.vividsolutions.jts.geom.LineString jtsLineString) {
        LinearRingTypeType ring=new LinearRingTypeType();
        for(com.vividsolutions.jts.geom.Coordinate jtsCoord:jtsLineString.getCoordinates()) {
            DirectPositionTypeType pos=new DirectPositionTypeType();
            pos.getValue().add(jtsCoord.x);
            pos.getValue().add(jtsCoord.y);
            ring.getPosOrPointPropertyOrPointRep().add(new Pos(pos));
        }
        return new LinearRing(ring);
    }

}
