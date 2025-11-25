CREATE OR REPLACE VIEW vista_sabana_datos AS
SELECT 
	v."ID_Oportunidad",
	c."Nombre_Empresa",
	c."Industria",
	v."Monto",
	v."Etapa",
	v."Categoria_Deal",
	v."Dias_Ciclo_Ventas"
FROM fact_ventas v
INNER JOIN dim_clientes c ON v."ID_Cliente" = c."ID_Cliente";

-- Probamos la vista
SELECT * FROM vista_sabana_datos;