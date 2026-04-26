package lsi.ubu.servicios;

import java.sql.SQLException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * GestionDonacionesSangreException:
 * Implementa las excepciones contextualizadas de la transacciones
 * de gestion de medicos
 * 
 * @author <a href="mailto:jmaudes@ubu.es">Jesus Maudes</a>
 * @author <a href="mailto:rmartico@ubu.es">Raul Marticorena</a>
 * @author <a href="mailto:pgdiaz@ubu.es">Pablo Garcia</a>
 * @author <a href="mailto:srarribas@ubu.es">Sandra Rodriguez</a>
 * @version 1.5
 * @since 1.0 
 */
public class GestionDonacionesSangreException extends SQLException {

	private static final long serialVersionUID = 1L;
	
	public static final int DONANTE_NO_EXISTE = 1;
	public static final int TIPO_SANGRE_NO_EXISTE = 2;
	public static final int HOSPITAL_NO_EXISTE = 3;
	public static final int DONANTE_EXCEDE = 4;
	public static final int VALOR_CANTIDAD_DONACION_INCORRECTO = 5;
	public static final int VALOR_CANTIDAD_TRASPASO_INCORRECTO = 6;
	public static final int VALOR_RESERVA_INCORRECTO = 7;
	

	private int codigo; // = -1;
	private String mensaje;

	private static Logger l = LoggerFactory.getLogger(GestionDonacionesSangreException.class);	

	public GestionDonacionesSangreException(int code) {
		codigo = code;
		String mensaje = null;

		switch (code) {
		case DONANTE_NO_EXISTE:
			mensaje = "Donante inexistente";
			break;
		case TIPO_SANGRE_NO_EXISTE:
			mensaje = "Tipo Sangre inexistente";
			break;
		case HOSPITAL_NO_EXISTE:
			mensaje = "Hospital ocupado";
			break;
		case DONANTE_EXCEDE:
			mensaje = "Donante excede el cupo de donación”.";
			break;
		case VALOR_CANTIDAD_DONACION_INCORRECTO:
			mensaje = "Valor de cantidad de donación incorrecto";
			break;
		case VALOR_CANTIDAD_TRASPASO_INCORRECTO:
			mensaje = "Valor de cantidad de traspaso por debajo de lo requerido";
			break;	
		case VALOR_RESERVA_INCORRECTO:
			mensaje = "Valor de cantidad de reserva por debajo de lo requerido";
			break;				
		}					

		l.error(mensaje);

		// Traza_de_pila
		for (StackTraceElement ste : Thread.currentThread().getStackTrace()) {
			l.info(ste.toString());
		}

	}

	@Override
	public String getMessage() { // Redefinicion del metodo de la clase
									// Exception
		return mensaje;
	}

	@Override
	public int getErrorCode() { // Redefinicion del metodo de la clase
								// SQLException
		return codigo;
	}

}
