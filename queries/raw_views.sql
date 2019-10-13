CREATE OR REPLACE VIEW ws.v_ai_pipeleak_raw_leak AS
 SELECT *
   FROM ( SELECT a_1.id,
            av.data,
                CASE
                    WHEN av.data IS NOT NULL THEN (av.data - at.builtdate)::numeric / 365.0
                    ELSE NULL::numeric
                END AS age,
            at.arc_id,
            at.staticpress,
            at.builtdate,
            at.matcat_id,
            at.pnom,
            at.dnom,
            at.depth,
            at.slope,
            at.expl_id,
            at.minsector,
            at.dqa,
            at.dma_id,
            at.presszonecat_id,
            at.workcat_id,
            at.soilcat_id,
            at.pavementcat,
            a_1.parameter_id,
            av.provocada,
            b.startdate
           FROM ws.om_visit_event a_1
             JOIN ws.om_visit b ON b.id = a_1.visit_id
             JOIN ws.om_visit_x_arc c USING (visit_id)
             JOIN ( SELECT arc.arc_id,
                    5 AS staticpress,
                    arc.builtdate,
                    cat_arc.matcat_id,
                        CASE
                            WHEN cat_arc.pnom::text = '0'::text THEN '10'::character varying
                            ELSE cat_arc.pnom
                        END AS pnom,
                    cat_arc.dnom,
                    1 AS depth,
                        CASE
                            WHEN ((n1.elevation - n2.elevation)::double precision / st_length(arc.the_geom)) IS NULL THEN 1.0::double precision
                            ELSE abs((n1.elevation - n2.elevation)::double precision / st_length(arc.the_geom)) * 100::double precision
                        END AS slope,
                    arc.expl_id,
                    arc.arc_id AS minsector,
                    arc.dma_id AS dqa,
                    arc.dma_id,
                    arc.presszonecat_id,
                    arc.workcat_id,
                    arc.soilcat_id,
                    1 AS pavementcat
                   FROM ws.arc
                     LEFT JOIN ws.cat_arc ON cat_arc.id::text = arc.arccat_id::text
                     LEFT JOIN ws.node n1 ON n1.node_id::text = arc.node_1::text
                     LEFT JOIN ws.node n2 ON n2.node_id::text = arc.node_2::text) at USING (arc_id)
             LEFT JOIN ws.aigues_avaries av ON av.visit_id = b.id
          WHERE a_1.parameter_id::text = 'avaria_tram'::text AND av.provocada::text <> 'S'::text OR av.provocada IS NULL AND at.dnom::numeric > 0::numeric) a
     LEFT JOIN ( SELECT connec.arc_id,
            count(*) AS n_connec,
            sum(v_aigues_consums."any") AS consum2018
           FROM ws.v_aigues_consums
             LEFT JOIN ws.connec USING (connec_id)
          GROUP BY connec.arc_id) ac USING (arc_id)
     LEFT JOIN ( SELECT arc.arc_id,
            st_length(arc.the_geom) AS length
           FROM ws.arc
          ORDER BY (st_length(arc.the_geom))) ad USING (arc_id)
     LEFT JOIN ( SELECT om_visit_x_arc.arc_id,
            count(*) AS prev_broken
           FROM ws.om_visit_x_arc
             JOIN ws.om_visit_event USING (visit_id)
          WHERE om_visit_event.parameter_id::text = 'avaria_tram'::text
          GROUP BY om_visit_x_arc.arc_id
          ORDER BY (count(*)) DESC) ae USING (arc_id);



CREATE OR REPLACE VIEW ws.v_ai_pipeleak_raw_noleak AS
 SELECT (a.data::date - a.builtdate)::numeric / 365.0 AS age,
    a.arc_id,
    a.data,
    a.staticpress,
    a.builtdate,
    a.matcat_id,
    a.pnom,
    a.dnom,
    a.depth,
    a.slope,
    a.expl_id,
    a.minsector,
    a.dqa,
    a.dma_id,
    a.presszonecat_id,
    a.workcat_id,
    a.soilcat_id,
    a.pavementcat,
    ac.n_connec,
    ac.consum2018,
    ad.length
   FROM ( SELECT arc.builtdate + random() * ('2018-12-31 00:00:00'::timestamp without time zone - arc.builtdate::timestamp without time zone) AS data,
            arc.arc_id,
            5 AS staticpress,
            arc.builtdate,
            cat_arc.matcat_id,
                CASE
                    WHEN cat_arc.pnom::text = '0'::text THEN '10'::character varying
                    ELSE cat_arc.pnom
                END AS pnom,
            cat_arc.dnom,
            1 AS depth,
                CASE
                    WHEN ((n1.elevation - n2.elevation)::double precision / st_length(arc.the_geom)) IS NULL THEN 1.0::double precision
                    ELSE abs(n1.elevation - n2.elevation)::double precision / st_length(arc.the_geom) * 100::double precision
                END AS slope,
            arc.expl_id,
            arc.arc_id AS minsector,
            arc.dma_id AS dqa,
            arc.dma_id,
            arc.presszonecat_id,
            arc.workcat_id,
            arc.soilcat_id,
            1 AS pavementcat
           FROM ws.arc
             JOIN ws.cat_arc ON cat_arc.id::text = arc.arccat_id::text
             JOIN ws.node n1 ON n1.node_id::text = arc.node_1::text
             JOIN ws.node n2 ON n2.node_id::text = arc.node_2::text
          WHERE NOT (arc.arc_id::text IN ( SELECT c.arc_id
                   FROM ws.om_visit_event a_1
                     JOIN ws.om_visit b ON b.id = a_1.visit_id
                     JOIN ws.om_visit_x_arc c USING (visit_id)
                  WHERE a_1.parameter_id::text = 'avaria_tram'::text))) a
     LEFT JOIN ( SELECT connec.arc_id,
            count(*) AS n_connec,
            sum(v_aigues_consums."any") AS consum2018
           FROM ws.v_aigues_consums
             LEFT JOIN ws.connec USING (connec_id)
          GROUP BY connec.arc_id) ac USING (arc_id)
     LEFT JOIN ( SELECT arc.arc_id,
            st_length(arc.the_geom) AS length
           FROM ws.arc
          ORDER BY (st_length(arc.the_geom))) ad USING (arc_id)
  WHERE a.matcat_id IS NOT NULL AND a.builtdate > '1900-01-01'::date AND a.dnom::numeric > 0.0;
  
  
  -- All
CREATE OR REPLACE VIEW ws.v_ai_pipeleak_raw_all AS

SELECT (now()::date - a.builtdate)::numeric / 365.0 AS age,
    a.arc_id,
    null as data,
    null as staticpress,
    a.builtdate,
    cat_arc.matcat_id,
              CASE
                    WHEN cat_arc.pnom::text = '0'::text THEN '10'::character varying
                    ELSE cat_arc.pnom
                END AS pnom,
    cat_arc.dnom,
    1 as depth,
              CASE
                    WHEN ((n1.elevation - n2.elevation)::double precision / st_length(a.the_geom)) IS NULL THEN 1.0::double precision
                    ELSE abs(n1.elevation - n2.elevation)::double precision / st_length(a.the_geom) * 100::double precision
                END AS slope,
    a.expl_id,
    null AS minsector,
    a.dma_id AS dqa,
    a.dma_id,
    a.presszonecat_id,
    a.workcat_id,
    a.soilcat_id,
    1 as pavementcat,
    ac.n_connec,
    ac.consum2018,
    ad.length
   FROM  ws.arc a
             JOIN ws.cat_arc ON cat_arc.id::text = a.arccat_id::text
             JOIN ws.node n1 ON n1.node_id::text = a.node_1::text
             JOIN ws.node n2 ON n2.node_id::text = a.node_2::text
      LEFT JOIN ( SELECT connec.arc_id,
            count(*) AS n_connec,
            sum(v_aigues_consums."any") AS consum2018
           FROM ws.v_aigues_consums
             LEFT JOIN ws.connec USING (connec_id)
          GROUP BY connec.arc_id) ac on ac.arc_id=a.arc_id
     LEFT JOIN ( SELECT arc.arc_id,
            st_length(arc.the_geom) AS length
           FROM ws.arc
          ORDER BY (st_length(arc.the_geom))) ad ON ad.arc_id=a.arc_id
  WHERE cat_arc.matcat_id IS NOT NULL AND a.builtdate > '1900-01-01'::date AND cat_arc.dnom::numeric > 0.0;


  
  
