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
    public SurfacePropertyTypeType convertSurface(com.vividsolutions.jts.geom.Geometry geometry) {
        if(geometry instanceof com.vividsolutions.jts.geom.Polygon) {
            SurfacePropertyTypeType surfaceProperty=new SurfacePropertyTypeType();
            surfaceProperty.setAbstractSurface(convertPolygon((com.vividsolutions.jts.geom.Polygon)geometry));
            return surfaceProperty;
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
            Pos pos = createPos(jtsCoord);
            ring.getPosOrPointPropertyOrPointRep().add(pos);
        }
        return new LinearRing(ring);
    }
    public Pos createPos(com.vividsolutions.jts.geom.Coordinate jtsCoord) {
        DirectPositionTypeType directPos = createDirectPositionType(jtsCoord);
        Pos pos = new Pos(directPos);
        return pos;
    }
    public DirectPositionTypeType createDirectPositionType(com.vividsolutions.jts.geom.Coordinate jtsCoord) {
        DirectPositionTypeType pos=new DirectPositionTypeType();
        pos.getValue().add(jtsCoord.x);
        pos.getValue().add(jtsCoord.y);
        return pos;
    }
    public PointPropertyTypeType createPointPropertyType(com.vividsolutions.jts.geom.Coordinate jtsCoord) {
        Pos pos=createPos(jtsCoord);
        PointTypeType point=new PointTypeType();
        point.setPos(pos);
        Point pointEle=new Point(point);
        PointPropertyTypeType ret=new PointPropertyTypeType();
        ret.setPoint(pointEle);
        return ret;
    }

}
