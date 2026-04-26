package lsi.ubu.solucion;

import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Date;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import lsi.ubu.util.ExecuteScript;
import lsi.ubu.util.PoolDeConexiones;
import lsi.ubu.servicios.GestionDonacionesSangreException;
import lsi.ubu.servicios.Misc;

public class GestionDonacionesSangre {

    /*
     * *****************************************************************************
     * ************** Logger y path necesario para nuestro script de SQL; para tener
     * un estado inicial consistente
     * *****************************************************************************
     * **************
     */
    private static Logger logger = LoggerFactory.getLogger(GestionDonacionesSangre.class);
    private static final String script_path = "sql/";

    /*
     * *****************************************************************************
     * ************** Ejecutar práctica
     * *****************************************************************************
     * **************
     */
    public static void main(String[] args) throws SQLException {
        tests();

        System.out.println("FIN.............");
    }

    /*
     * *****************************************************************************
     * ************** Procedimientos implementados
     * *****************************************************************************
     * **************
     */

    /*
     * Añadirá una entrada en la tabla donaciones con la información relativa a la
     * donación realizada e incrementará la reserva del hospital correspondiente.
     * Hay que tener en cuenta que el máximo de donación es de 0,45 litros y que una
     * persona no podrá donar más de una vez cada 15 días.
     */
    public static void realizar_donacion(String m_NIF, float m_Cantidad, int m_ID_Hospital, Date m_Fecha_Donacion)
            throws SQLException {

        // ¿Es correcta la cantidad a donar?
        if (m_Cantidad > 0.45f || m_Cantidad < 0) {
            throw new GestionDonacionesSangreException(
                    GestionDonacionesSangreException.VALOR_CANTIDAD_DONACION_INCORRECTO);
        }

        PoolDeConexiones pool = PoolDeConexiones.getInstance();
        Connection cnx = null;

        PreparedStatement checkNifSt = null;
        PreparedStatement yaHaDonadoSt = null;
        PreparedStatement selectTipoSangreSt = null;
        PreparedStatement insertDonacionSt = null;
        PreparedStatement updateReservaSt = null;
        ResultSet rs = null;

        try {
            cnx = pool.getConnection();
            cnx.setAutoCommit(false); 

            ///////////////////////////////////////////////////
            // Comprobaciones defensivas
            // No hay otra forma de comprobar esto. Debemos al menos
            // realizar una consulta a la base de datos para saber
            // si ha realizado una donacion en los ultimos 15 días.
            ///////////////////////////////////////////////////

            // Comprobar si el NIF existe en la tabla donantes
            checkNifSt = cnx.prepareStatement("SELECT 1 FROM donante WHERE NIF = ?");
            checkNifSt.setString(1, m_NIF);
            rs = checkNifSt.executeQuery();
            if (!rs.next()) {
                throw new GestionDonacionesSangreException(GestionDonacionesSangreException.DONANTE_NO_EXISTE);
            }
            rs.close(); 

            // ¿Ha donado en los ultimos 15 dias?
            yaHaDonadoSt = cnx.prepareStatement("""
                    SELECT COUNT(NIF_DONANTE)
                    FROM DONACION
                    WHERE
                        FECHA_DONACION > TRUNC(SYSDATE) - 15
                        AND
                        NIF_DONANTE = ?
                    """);
            yaHaDonadoSt.setString(1, m_NIF);
            rs = yaHaDonadoSt.executeQuery();
            rs.next();
            int yaHaDonadoVeces = rs.getInt(1);
            rs.close();

            if (yaHaDonadoVeces > 0) {
                throw new GestionDonacionesSangreException(GestionDonacionesSangreException.DONANTE_EXCEDE);
            }

            ///////////////////////////////////////////////////
            // Insertando la donación
            ///////////////////////////////////////////////////

            // Necesitamos saber el tipo de sangre para actualizar las reservas.
            // Como tenemos el NIF del donante podemos buscar su tipo en la tabla de donantes.
            selectTipoSangreSt = cnx.prepareStatement("""
                    SELECT id_tipo_sangre
                    FROM donante
                    WHERE NIF = ?
                    """);
            selectTipoSangreSt.setString(1, m_NIF);
            rs = selectTipoSangreSt.executeQuery();
            if (!rs.next()) {
                throw new GestionDonacionesSangreException(GestionDonacionesSangreException.TIPO_SANGRE_NO_EXISTE);
            }
            String tipoSangre = rs.getString("id_tipo_sangre");
            rs.close();

            // Insertar donación
            insertDonacionSt = cnx.prepareStatement("""
                    INSERT INTO donacion(id_donacion, nif_donante, cantidad, fecha_donacion)
                    VALUES (seq_donacion.nextval, ?, ?, ?)
                    """);
            insertDonacionSt.setString(1, m_NIF);
            insertDonacionSt.setFloat(2, m_Cantidad);
            insertDonacionSt.setDate(3, m_Fecha_Donacion);
            insertDonacionSt.executeUpdate();

            // Actualizamos las reservas con los datos + el tipo de sangre consultado antes
            updateReservaSt = cnx.prepareStatement("""
                    UPDATE reserva_hospital
                    SET cantidad = cantidad + ?
                    WHERE
                        id_tipo_sangre = ?
                        AND
                        id_hospital = ?
                    """);
            updateReservaSt.setFloat(1, m_Cantidad);
            updateReservaSt.setString(2, tipoSangre);
            updateReservaSt.setInt(3, m_ID_Hospital);
            int actualizaciones = updateReservaSt.executeUpdate();
            
            if (actualizaciones < 1) {
                throw new GestionDonacionesSangreException(GestionDonacionesSangreException.HOSPITAL_NO_EXISTE);
            }

            cnx.commit();

        } catch (GestionDonacionesSangreException e) {
            if (cnx != null) cnx.rollback();
            throw e;
        } catch (SQLException e) {
            if (cnx != null) cnx.rollback();
            logger.error("Error SQL en realizar_donacion: " + e.getMessage());
            throw e;
        } finally {
            // Liberamos todos los recursos utilizados con un bloque finally (Norma del PDF)
            try { if (rs != null) rs.close(); } catch (SQLException e) { }
            try { if (checkNifSt != null) checkNifSt.close(); } catch (SQLException e) {}
            try { if (yaHaDonadoSt != null) yaHaDonadoSt.close(); } catch (SQLException e) {}
            try { if (selectTipoSangreSt != null) selectTipoSangreSt.close(); } catch (SQLException e) {}
            try { if (insertDonacionSt != null) insertDonacionSt.close(); } catch (SQLException e) {}
            try { if (updateReservaSt != null) updateReservaSt.close(); } catch (SQLException e) {}
            try { if (cnx != null) cnx.close(); } catch (SQLException e) {}
        }
    }

    /*
     * Borra de la tabla traspaso los traspasos por id_tipo_sangre,
     * id_hospital_origen, id_hospital_destino y fecha_traspaso. Hay que tener en
     * cuenta que se debe restar el contenido del campo cantidad de cada traspaso
     * del campo cantidad de la reserva asociada al hospital destino y sumar dicha
     * cantidad a la reserva asociada al hospital origen.
     */
    public static void anular_traspaso(int m_ID_Tipo_Sangre, int m_ID_Hospital_Origen, int m_ID_Hospital_Destino, Date m_Fecha_Traspaso) throws SQLException {
        PoolDeConexiones pool = PoolDeConexiones.getInstance();
        Connection cnx = null;
        PreparedStatement selectTraspasoSt = null;
        ResultSet rs = null;
        
        try {
            cnx = pool.getConnection();
            cnx.setAutoCommit(false);
            
            selectTraspasoSt = cnx.prepareStatement("""
                    SELECT id_traspaso, id_hospital_origen, id_hospital_destino, cantidad
                    FROM traspaso
                    WHERE 
                        id_tipo_sangre = ?      AND
                        id_hospital_origen = ?  AND
                        id_hospital_destino = ? AND
                        fecha_traspaso = ?
                    """);
            selectTraspasoSt.setInt(1, m_ID_Tipo_Sangre);
            selectTraspasoSt.setInt(2, m_ID_Hospital_Origen);
            selectTraspasoSt.setInt(3, m_ID_Hospital_Destino);
            selectTraspasoSt.setDate(4, m_Fecha_Traspaso);
            
            rs = selectTraspasoSt.executeQuery();
            
            // Si no encontramos ese traspaso por el motivo que sea, simplemente se termina.
            // No necesitamos comprobar nada ,incluso si no existen los valores. Porque no
            // estamos tocando nada. No debemos borrar nada.
            if(!rs.next()) {
                return;
            }
            
            // Si hemos encontrado algo llegamos hasta aquí.
            // Ahora hay que restar en destino, sumar en origen y quitar la transacción.
            int idTraspaso = rs.getInt("id_traspaso");
            int idHospitalOrigen = rs.getInt("id_hospital_origen");
            int idHospitalDestino = rs.getInt("id_hospital_destino");
            float cantidad = rs.getFloat("cantidad");
            
            // TODO: Terminar la implementación (Restar, Sumar, Delete)
            
            cnx.commit();
        } catch (GestionDonacionesSangreException e) {
            if (cnx != null) cnx.rollback();
            throw e;
        } catch (SQLException e) {
            if (cnx != null) cnx.rollback();
            logger.error("Error SQL en anular_traspaso: " + e.getMessage());
            throw e;
        } finally {
            try { if (rs != null) rs.close(); } catch (SQLException e) {}
            try { if (selectTraspasoSt != null) selectTraspasoSt.close(); } catch (SQLException e) {}
            try { if (cnx != null) cnx.close(); } catch (SQLException e) {}
        }
    }

    /*
     * Mostrará por salida estándar un listado ordenado por id_hospital _destino y
     * fecha_traspaso de los traspasos realizados de un determinado tipo de sangre
     * (el parámetro recibe la descripción del tipo de sangre que se encuentra en la
     * tabla tipo_sangre). Se deben mostrar el contenido de la tabla traspasos,
     * hospital, reserva_hospital y tipo_sangre.
     */
    public static void consulta_traspasos(String m_Tipo_Sangre) throws SQLException {
        // TODO: Implementar por la compañera
    }

    /*
     * *****************************************************************************
     * ************** Pruebas para procedimientos
     * *****************************************************************************
     * **************
     */

    /*
     * *****************************************************************************
     * ************** Funciones para montar entorno de pruebas y lanzamiento de
     * pruebas
     * *****************************************************************************
     * **************
     */

    /*
     * DROP + CREATE + POPULATE de las tablas
     */
    static public void creaTablas() {
        ExecuteScript.run(script_path + "gestion_donaciones_sangre.sql");
    }

    /*
     * Probanado que nuestros procedimientos funcionen.
     */
    static void tests() throws SQLException {
        creaTablas();

        PoolDeConexiones pool = PoolDeConexiones.getInstance();

        // Relatar caso por caso utilizando el siguiente procedure para inicializar los datos
        CallableStatement cll_reinicia = null;
        Connection conn = null;

        try {
            // Reinicio filas
            conn = pool.getConnection();
            cll_reinicia = conn.prepareCall("{call inicializa_test}");
            cll_reinicia.execute();

            ///////////////////////////////////////////////////
            // TESTS (Casos Normales y Extremos)
            ///////////////////////////////////////////////////
            
            // Nota: Asumimos un NIF '12345678A' y '87654321B' válidos insertados por el script SQL. 
            // Si en tu SQL son distintos, modifícalos en los tests correspondientes.
            
            System.out.println("\n=== PRUEBAS DE EJECUCION NORMAL ===");

            // CASO NORMAL 1: Donación válida
            try {
                System.out.println("TEST NORMAL 1: Intentando donacion valida...");
                GestionDonacionesSangre.realizar_donacion("12345678A", 0.30f, 1, new Date(Misc.getCurrentDate().getTime()));
                System.out.println("EXITO: Donacion registrada correctamente.");
            } catch (GestionDonacionesSangreException e) {
                System.out.println("FALLO: No se esperaba excepcion -> " + e.getMessage());
            }

            // CASO NORMAL 2: Anulación de traspaso (Cuando esté hecha)
            try {
                System.out.println("TEST NORMAL 2: Intentando anular traspaso valido...");
                GestionDonacionesSangre.anular_traspaso(1, 1, 2, new Date(Misc.getCurrentDate().getTime()));
                System.out.println("EXITO: Metodo anular_traspaso ejecutado sin fallos.");
            } catch (GestionDonacionesSangreException e) {
                System.out.println("FALLO: No se esperaba excepcion -> " + e.getMessage());
            }

            // CASO NORMAL 3: Consulta de traspasos (Cuando esté hecha)
            try {
                System.out.println("TEST NORMAL 3: Intentando consulta de traspasos...");
                GestionDonacionesSangre.consulta_traspasos("A Positivo");
                System.out.println("EXITO: Metodo consulta_traspasos ejecutado sin fallos.");
            } catch (GestionDonacionesSangreException e) {
                System.out.println("FALLO: No se esperaba excepcion -> " + e.getMessage());
            }

            System.out.println("\n=== PRUEBAS DE CASOS EXTREMOS (EXCEPCIONES) ===");

            // EXCEPCIÓN 1: Donante inexistente
            try {
                System.out.println("TEST EXC 1: Donante inexistente...");
                GestionDonacionesSangre.realizar_donacion("99999999Z", 0.30f, 1, new Date(Misc.getCurrentDate().getTime()));
                System.out.println("FALLO: Deberia haber saltado la excepcion DONANTE_NO_EXISTE.");
            } catch (GestionDonacionesSangreException e) {
                if (e.getErrorCode() == GestionDonacionesSangreException.DONANTE_NO_EXISTE) {
                    System.out.println("EXITO: Excepcion capturada -> " + e.getMessage());
                } else {
                    System.out.println("FALLO: Salto otra excepcion -> " + e.getMessage());
                }
            }

            // EXCEPCIÓN 2: Tipo de Sangre Inexistente
            try {
                System.out.println("TEST EXC 2: Tipo de Sangre Inexistente...");
                GestionDonacionesSangre.consulta_traspasos("Sangre Alienigena");
                System.out.println("FALLO (Aviso): Deberia fallar cuando la compañera implemente el metodo con excepciones.");
            } catch (GestionDonacionesSangreException e) {
                if (e.getErrorCode() == GestionDonacionesSangreException.TIPO_SANGRE_NO_EXISTE) {
                    System.out.println("EXITO: Excepcion capturada -> " + e.getMessage());
                } else {
                    System.out.println("FALLO: Salto otra excepcion -> " + e.getMessage());
                }
            }

            // EXCEPCIÓN 3: Hospital Inexistente
            try {
                System.out.println("TEST EXC 3: Hospital Inexistente...");
                GestionDonacionesSangre.realizar_donacion("87654321B", 0.30f, 9999, new Date(Misc.getCurrentDate().getTime()));
                System.out.println("FALLO: Deberia haber saltado la excepcion HOSPITAL_NO_EXISTE.");
            } catch (GestionDonacionesSangreException e) {
                if (e.getErrorCode() == GestionDonacionesSangreException.HOSPITAL_NO_EXISTE) {
                    System.out.println("EXITO: Excepcion capturada -> " + e.getMessage());
                } else {
                    System.out.println("FALLO: Salto otra excepcion -> " + e.getMessage());
                }
            }

            // EXCEPCIÓN 4: Donante excede el cupo (15 días)
            try {
                System.out.println("TEST EXC 4: Donante excede cupo (15 dias)...");
                // Como ya donó 12345678A en la PRUEBA NORMAL 1, volver a donar debe fallar.
                GestionDonacionesSangre.realizar_donacion("12345678A", 0.20f, 1, new Date(Misc.getCurrentDate().getTime()));
                System.out.println("FALLO: Deberia haber saltado la excepcion DONANTE_EXCEDE.");
            } catch (GestionDonacionesSangreException e) {
                if (e.getErrorCode() == GestionDonacionesSangreException.DONANTE_EXCEDE) {
                    System.out.println("EXITO: Excepcion capturada -> " + e.getMessage());
                } else {
                    System.out.println("FALLO: Salto otra excepcion -> " + e.getMessage());
                }
            }

            // EXCEPCIÓN 5: Cantidad de donación incorrecta
            try {
                System.out.println("TEST EXC 5: Valor de donacion incorrecto (> 0.45)...");
                GestionDonacionesSangre.realizar_donacion("87654321B", 0.60f, 1, new Date(Misc.getCurrentDate().getTime()));
                System.out.println("FALLO: Deberia haber saltado la excepcion VALOR_CANTIDAD_DONACION_INCORRECTO.");
            } catch (GestionDonacionesSangreException e) {
                if (e.getErrorCode() == GestionDonacionesSangreException.VALOR_CANTIDAD_DONACION_INCORRECTO) {
                    System.out.println("EXITO: Excepcion capturada -> " + e.getMessage());
                } else {
                    System.out.println("FALLO: Salto otra excepcion -> " + e.getMessage());
                }
            }

            // EXCEPCIÓN 6: Cantidad de traspaso por debajo de lo requerido
            try {
                System.out.println("TEST EXC 6: Valor traspaso por debajo de lo requerido...");
                GestionDonacionesSangre.anular_traspaso(1, 1, 2, new Date(Misc.getCurrentDate().getTime())); 
                System.out.println("FALLO (Aviso): Deberia fallar cuando la compañera termine anular_traspaso forzando un error de cantidad.");
            } catch (GestionDonacionesSangreException e) {
                if (e.getErrorCode() == GestionDonacionesSangreException.VALOR_CANTIDAD_TRASPASO_INCORRECTO) {
                    System.out.println("EXITO: Excepcion capturada -> " + e.getMessage());
                } else {
                    System.out.println("FALLO: Salto otra excepcion -> " + e.getMessage());
                }
            }

        } catch (SQLException e) {
            logger.error("Error SQL general en los tests: " + e.getMessage());
        } finally {
            try { if (cll_reinicia != null) cll_reinicia.close(); } catch (SQLException e) {}
            try { if (conn != null) conn.close(); } catch (SQLException e) {}
        }
    }
}