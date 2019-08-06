/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.drcarlosamaral.MongoImporter;

/**
 *
 * @author Nicholas DiPiazza
 */
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.Calendar;
import java.util.Date;

public class DateUtils {

  public static Date asDate(LocalDate localDate) {
    return Date.from(localDate.atStartOfDay().atZone(ZoneId.systemDefault()).toInstant());
  }

  public static Date asDate(LocalDateTime localDateTime) {
    return Date.from(localDateTime.atZone(ZoneId.systemDefault()).toInstant());
  }

  public static LocalDate asLocalDate(Date date) {
    // return Instant.ofEpochMilli(date.getTime()).atOffset(ZoneOffset.UTC).toLocalDate();
    return Instant.ofEpochMilli(date.getTime()).atZone(ZoneId.systemDefault()).toLocalDate();
  }

  public static LocalDateTime asLocalDateTime(Date date) {
    //  return Instant.ofEpochMilli(date.getTime()).atOffset(ZoneOffset.UTC).toLocalDateTime();
    return Instant.ofEpochMilli(date.getTime()).atZone(ZoneId.systemDefault()).toLocalDateTime();
  }
  /**
   * Tem que passar uma data no formato "dd/MM/aaaa" ou "dd/MM/aa"
   * @param dataString
   * @return Retorna um Date pronto para o MongoDB ou null se não conseguir converter
   */
  public static Date asDate(String dataString) {
	  if (dataString == null || 
			  dataString.equals("") ||
			  !(dataString.length() == 10 || dataString.length() == 8)) return null;
	  int dia = Integer.parseInt(dataString.substring(0,2));
	  int mes = Integer.parseInt(dataString.substring(3,5));
	  int ano = Integer.parseInt(dataString.substring(6,8));
	  if (dataString.length() == 10) ano = Integer.parseInt(dataString.substring(6,10));
	  if (dataString.length() == 8) {
		  if (LocalDate.now().minusYears(ano + 2000).getYear() >= 0) {
			  ano = ano + 2000;
		  } else {
			  ano = ano + 1900;
		  }
	  }
	  Calendar ca = Calendar.getInstance();
	  ca.set(ano, mes, dia);
	  return ca.getTime();
  }
}