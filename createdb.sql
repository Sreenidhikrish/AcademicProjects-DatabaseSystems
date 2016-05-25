Create table building(building_ID varchar2(10) Primary Key,
building_name Varchar2(50),building_shape SDO_GEOMETRY,
building_on_fire varchar2(5));

INSERT INTO USER_SDO_GEOM_METADATA(TABLE_NAME, COLUMN_NAME, DIMINFO, SRID)VALUES
('building','building_shape',SDO_DIM_ARRAY(SDO_DIM_ELEMENT('X', 0, 100, 1),SDO_DIM_ELEMENT('Y', 0, 100, 1)),NULL);

CREATE INDEX building_index ON building(building_shape) INDEXTYPE IS MDSYS.SPATIAL_INDEX;
        
Create table hydrant(
hydrant_ID varchar2(10) Primary Key, 
hydrant_coord SDO_GEOMETRY);

INSERT INTO USER_SDO_GEOM_METADATA (TABLE_NAME, COLUMN_NAME, DIMINFO, SRID)VALUES
('hydrant','hydrant_coord',SDO_DIM_ARRAY(SDO_DIM_ELEMENT('X', 0, 100, 1),SDO_DIM_ELEMENT('Y', 0, 100, 1)),NULL);

CREATE INDEX hydrant_index ON hydrant(hydrant_coord) INDEXTYPE IS MDSYS.SPATIAL_INDEX;   
                           
                      