



drop INDEX building_index;

drop INDEX hydrant_index;




DELETE FROM USER_SDO_GEOM_METADATA WHERE TABLE_NAME ='BUILDING';

DELETE FROM USER_SDO_GEOM_METADATA WHERE TABLE_NAME = 'HYDRANT';

drop table building;

drop table hydrant;


