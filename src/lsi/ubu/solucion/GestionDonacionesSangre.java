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

    private static Logger logger = LoggerFactory.getLogger(GestionDonacionesSangre.class);
    private static final String script_path = "sql/";

    public static void main(String[] args) throws SQLException {
        tests();
        System.out.println("FIN.............");
    }

    public static void realizar_donacion(String m_NIF, float m_Cantidad, int m_ID_Hospital, Date m_Fecha_Donacion) throws SQLException {
        PoolDeConexiones pool = PoolDeConexiones.getInstance();
        Connection cnx = pool.getConnection();

        // 1. Comprobaciones de cantidad
        if (m_Cantidad > 0.45 || m_Cantidad < 0) {
            cnx.close();
            throw new GestionDonacionesSangreException(GestionDonacionesSangreException.VALOR_CANTIDAD_DONACION_INCORRECTO);
        }

        // 2. Comprobar si ha donado en los ultimos 15 dias
        PreparedStatement yaHaDonadoSt = cnx.prepareStatement("""
                SELECT COUNT(NIF_DONANTE)
                FROM DONACION
                WHERE FECHA_DONACION > TRUNC(SYSDATE) - 15 AND NIF_DONANTE = ?
                """);
        yaHaDonadoSt.setString(1, m_NIF);
        ResultSet donacionesRealizadasRs = yaHaDonadoSt.executeQuery();
        donacionesRealizadasRs.next();
        int yaHaDonadoVeces = donacionesRealizadasRs.getInt(1);
        donacionesRealizadasRs.close();
        yaHaDonadoSt.close();

        if (yaHaDonadoVeces > 0) {
            cnx.close();
            throw new GestionDonacionesSangreException(GestionDonacionesSangreException.DONANTE_EXCEDE);
        }

        // 3. Insertar donación
        try {
            PreparedStatement insertDonacionSt = cnx.prepareStatement("""
                    INSERT INTO donacion(id_donacion, nif_donante, cantidad, fecha_donacion)
                    VALUES (seq_donacion.nextval, ?, ?, ?)
                    """);
            insertDonacionSt.setString(1, m_NIF);
            insertDonacionSt.setFloat(2, m_Cantidad);
            insertDonacionSt.setDate(3, m_Fecha_Donacion);
            insertDonacionSt.executeUpdate();
            insertDonacionSt.close();
        } catch (SQLException ex) {
            cnx.rollback();
            cnx.close();
            if (ex.getMessage().contains("parent key not found") || ex.getMessage().contains("DONACION_NIF_DONANTE_FK")) {
                throw new GestionDonacionesSangreException(GestionDonacionesSangreException.DONANTE_NO_EXISTE);
            } else {
                throw ex;
            }
        }

        // 4. Buscar tipo de sangre del donante
        PreparedStatement selectTipoSangreSt = cnx.prepareStatement("SELECT id_tipo_sangre FROM donante WHERE NIF = ?");
        selectTipoSangreSt.setString(1, m_NIF);
        ResultSet rs = selectTipoSangreSt.executeQuery();
        
        if (!rs.next()) {
            rs.close();
            selectTipoSangreSt.close();
            cnx.rollback();
            cnx.close();
            throw new GestionDonacionesSangreException(GestionDonacionesSangreException.TIPO_SANGRE_NO_EXISTE);
        }
        String tipoSangre = rs.getString("id_tipo_sangre");
        rs.close();
        selectTipoSangreSt.close();

        // 5. Actualizar reservas del hospital
        PreparedStatement updateReservaSt = cnx.prepareStatement("""
                UPDATE reserva_hospital
                SET cantidad = cantidad + ?
                WHERE id_tipo_sangre = ? AND id_hospital = ?
                """);
        updateReservaSt.setFloat(1, m_Cantidad);
        updateReservaSt.setString(2, tipoSangre);
        updateReservaSt.setInt(3, m_ID_Hospital);
        int actualizaciones = updateReservaSt.executeUpdate();
        updateReservaSt.close();

        if (actualizaciones < 1) {
            cnx.rollback();
            cnx.close();
            throw new GestionDonacionesSangreException(GestionDonacionesSangreException.HOSPITAL_NO_EXISTE);
        }

        cnx.commit();
        cnx.close();
    }


    public static void consulta_traspasos(String m_Tipo_Sangre) throws SQLException {
        PoolDeConexiones pool = PoolDeConexiones.getInstance();
        Connection cnx = null;
        PreparedStatement checkSangreSt = null;
        PreparedStatement st = null;
        ResultSet rsCheck = null;
        ResultSet rs = null;

        try {
            cnx = pool.getConnection();
            
            checkSangreSt = cnx.prepareStatement("SELECT 1 FROM tipo_sangre WHERE descripcion = ?");
            checkSangreSt.setString(1, m_Tipo_Sangre);
            rsCheck = checkSangreSt.executeQuery();
            if (!rsCheck.next()) {
                throw new GestionDonacionesSangreException(GestionDonacionesSangreException.TIPO_SANGRE_NO_EXISTE);
            }

            st = cnx.prepareStatement("""
                    SELECT
                        id_traspaso,
                        traspaso.cantidad,
                        fecha_traspaso,
                        descripcion,
                        nombre,
                        localidad,
                        reserva_hospital.cantidad AS cantidad_reserva
                    FROM traspaso
                    JOIN tipo_sangre ON tipo_sangre.id_tipo_sangre = traspaso.id_tipo_sangre
                    JOIN hospital ON traspaso.id_hospital_destino = hospital.id_hospital
                    JOIN reserva_hospital ON traspaso.id_hospital_destino = reserva_hospital.id_hospital
                    WHERE tipo_sangre.descripcion = ?
                    ORDER BY id_hospital_destino, fecha_traspaso
                    """);
            st.setString(1, m_Tipo_Sangre);
            rs = st.executeQuery();

            System.out.println("|id\t|c. traspaso\t|fecha\t\t|sangre\t\t|hospital\t\t\t\t\t|localidad\t\t|reservas");
            System.out.println("--------------------------------------------------------------------------------------------------------------------------------------------");

            while (rs.next()) {
                int id = rs.getInt("id_traspaso");
                float cantidadTraspaso = rs.getFloat("cantidad");
                Date fecha = rs.getDate("fecha_traspaso");
                String desc = rs.getString("descripcion");
                String nombre = rs.getString("nombre");
                String localidad = rs.getString("localidad");
                float cantidadReserva = rs.getFloat("cantidad_reserva");

                String s = String.format("|%d\t|%f\t|%s\t|%s\t|%s\t|%s\t\t|%f", id, cantidadTraspaso, fecha, desc, nombre, localidad, cantidadReserva);
                System.out.println(s);
            }
            System.out.println("--------------------------------------------------------------------------------------------------------------------------------------------");

        } catch (GestionDonacionesSangreException e) {
            throw e;
        } catch (SQLException ex) {
            logger.error("Error SQL en consulta_traspasos: " + ex.getMessage());
            throw ex;
        } finally {
            try { if (rsCheck != null) rsCheck.close(); } catch (SQLException e) {}
            try { if (rs != null) rs.close(); } catch (SQLException e) {}
            try { if (checkSangreSt != null) checkSangreSt.close(); } catch (SQLException e) {}
            try { if (st != null) st.close(); } catch (SQLException e) {}
            try { if (cnx != null) cnx.close(); } catch (SQLException e) {}
        }
    }


    public static void anular_traspaso(int m_ID_Tipo_Sangre, int m_ID_Hospital_Origen, int m_ID_Hospital_Destino, Date m_Fecha_Traspaso) throws SQLException {
        PoolDeConexiones pool = PoolDeConexiones.getInstance();
        Connection cnx = null;
        PreparedStatement selectTraspasoSt = null;
        PreparedStatement restarDestinoSt = null;
        PreparedStatement sumarOrigenSt = null;
        PreparedStatement borrarTraspasoSt = null;
        ResultSet rs = null;

        try {
            cnx = pool.getConnection();
            cnx.setAutoCommit(false);

            selectTraspasoSt = cnx.prepareStatement("""
                    SELECT id_traspaso, id_hospital_origen, id_hospital_destino, cantidad
                    FROM traspaso
                    WHERE id_tipo_sangre = ? AND id_hospital_origen = ? AND id_hospital_destino = ? AND fecha_traspaso = ?
                    """);
            selectTraspasoSt.setInt(1, m_ID_Tipo_Sangre);
            selectTraspasoSt.setInt(2, m_ID_Hospital_Origen);
            selectTraspasoSt.setInt(3, m_ID_Hospital_Destino);
            selectTraspasoSt.setDate(4, m_Fecha_Traspaso);
            rs = selectTraspasoSt.executeQuery();
            
            if (!rs.next()) {
                return;
            }
            
            int idTraspaso = rs.getInt("id_traspaso");
            int idHospitalOrigen = rs.getInt("id_hospital_origen");
            int idHospitalDestino = rs.getInt("id_hospital_destino");
            float cantidad = rs.getFloat("cantidad");

            if (cantidad < 0) {
                throw new GestionDonacionesSangreException(GestionDonacionesSangreException.VALOR_CANTIDAD_TRASPASO_INCORRECTO);
            }

            restarDestinoSt = cnx.prepareStatement("UPDATE reserva_hospital SET cantidad = cantidad - ? WHERE id_hospital = ? AND id_tipo_sangre = ?");
            restarDestinoSt.setFloat(1, cantidad);
            restarDestinoSt.setInt(2, idHospitalDestino);
            restarDestinoSt.setInt(3, m_ID_Tipo_Sangre);
            restarDestinoSt.executeUpdate();

            sumarOrigenSt = cnx.prepareStatement("UPDATE reserva_hospital SET cantidad = cantidad + ? WHERE id_hospital = ? AND id_tipo_sangre = ?");
            sumarOrigenSt.setFloat(1, cantidad);
            sumarOrigenSt.setInt(2, idHospitalOrigen);
            sumarOrigenSt.setInt(3, m_ID_Tipo_Sangre);
            sumarOrigenSt.executeUpdate();

            borrarTraspasoSt = cnx.prepareStatement("DELETE FROM traspaso WHERE id_traspaso = ?");
            borrarTraspasoSt.setInt(1, idTraspaso);
            borrarTraspasoSt.executeUpdate();

            cnx.commit();
            
        } catch (GestionDonacionesSangreException e) {
            if (cnx != null) cnx.rollback();
            throw e;
        } catch (SQLException ex) {
            if (cnx != null) cnx.rollback();
            logger.error("Error SQL en anular_traspaso: " + ex.getMessage());
            throw ex;
        } finally {
            try { if (rs != null) rs.close(); } catch (SQLException e) {}
            try { if (selectTraspasoSt != null) selectTraspasoSt.close(); } catch (SQLException e) {}
            try { if (restarDestinoSt != null) restarDestinoSt.close(); } catch (SQLException e) {}
            try { if (sumarOrigenSt != null) sumarOrigenSt.close(); } catch (SQLException e) {}
            try { if (borrarTraspasoSt != null) borrarTraspasoSt.close(); } catch (SQLException e) {}
            try { if (cnx != null) cnx.close(); } catch (SQLException e) {}
        }
    }

    static public void creaTablas() {
        ExecuteScript.run(script_path + "gestion_donaciones_sangre.sql");
    }

    static void tests() throws SQLException {
        creaTablas();
        PoolDeConexiones pool = PoolDeConexiones.getInstance();
        CallableStatement cll_reinicia = null;
        Connection conn = null;

        try {
            conn = pool.getConnection();
            cll_reinicia = conn.prepareCall("{call inicializa_test}");
            cll_reinicia.execute();

            System.out.println("\n=== PRUEBAS DE EJECUCION NORMAL ===");

            try {
                System.out.println("TEST NORMAL 1: Intentando donacion valida...");
                GestionDonacionesSangre.realizar_donacion("12345678A", 0.30f, 1, new Date(Misc.getCurrentDate().getTime()));
                System.out.println("EXITO: Donacion registrada correctamente.");
            } catch (GestionDonacionesSangreException e) {
                System.out.println("FALLO: No se esperaba excepcion -> " + e.getMessage());
            }

            try {
                System.out.println("TEST NORMAL 2: Intentando anular traspaso valido...");
                GestionDonacionesSangre.anular_traspaso(1, 1, 2, new Date(Misc.getCurrentDate().getTime()));
                System.out.println("EXITO: Metodo anular_traspaso ejecutado sin fallos.");
            } catch (GestionDonacionesSangreException e) {
                System.out.println("FALLO: No se esperaba excepcion -> " + e.getMessage());
            }

            try {
                System.out.println("TEST NORMAL 3: Intentando consulta de traspasos...");
                GestionDonacionesSangre.consulta_traspasos("A Positivo");
                System.out.println("EXITO: Metodo consulta_traspasos ejecutado sin fallos.");
            } catch (GestionDonacionesSangreException e) {
                System.out.println("FALLO: No se esperaba excepcion -> " + e.getMessage());
            }

            System.out.println("\n=== PRUEBAS DE CASOS EXTREMOS (EXCEPCIONES) ===");

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

            try {
                System.out.println("TEST EXC 2: Tipo de Sangre Inexistente...");
                GestionDonacionesSangre.consulta_traspasos("Sangre Alienigena");
                System.out.println("FALLO (Aviso): Deberia fallar.");
            } catch (GestionDonacionesSangreException e) {
                if (e.getErrorCode() == GestionDonacionesSangreException.TIPO_SANGRE_NO_EXISTE) {
                    System.out.println("EXITO: Excepcion capturada -> " + e.getMessage());
                } else {
                    System.out.println("FALLO: Salto otra excepcion -> " + e.getMessage());
                }
            }

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

            try {
                System.out.println("TEST EXC 4: Donante excede cupo (15 dias)...");
                GestionDonacionesSangre.realizar_donacion("12345678A", 0.20f, 1, new Date(Misc.getCurrentDate().getTime()));
                System.out.println("FALLO: Deberia haber saltado la excepcion DONANTE_EXCEDE.");
            } catch (GestionDonacionesSangreException e) {
                if (e.getErrorCode() == GestionDonacionesSangreException.DONANTE_EXCEDE) {
                    System.out.println("EXITO: Excepcion capturada -> " + e.getMessage());
                } else {
                    System.out.println("FALLO: Salto otra excepcion -> " + e.getMessage());
                }
            }

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

            try {
                System.out.println("TEST EXC 6: Valor traspaso por debajo de lo requerido...");
                GestionDonacionesSangre.anular_traspaso(3, 1, 2, new Date(Misc.getCurrentDate().getTime()));
                System.out.println("FALLO (Aviso): Deberia fallar si existiera un traspaso con cantidad negativa para esos datos.");
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