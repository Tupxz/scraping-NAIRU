**************************
*Importación de los datos*
**************************
cd "C:\Users\DELL\Documents\DSGE\Funcion_produccion"
clear all
import excel "C:\Users\DELL\Documents\DSGE\Funcion_produccion\PIB_CO 1.xlsx", firstrow
rename ValueBillions PIB
gen t=yq(Year,Quarter)
*replace amz=ln(amz)
format t %tq
tsset t
tsline PIB
*********
*Filtros*
*********
*Baxter y King
tsfilter bk bk_PIB = PIB, minperiod(6) maxperiod(32) smaorder(12) trend(bkt_PIB)
tsline PIB bkt_PIB
*Christiano y Fitzgerald
tsfilter cf cf_PIB = PIB, minperiod(6) maxperiod(32) drift trend(cft_PIB)
tsline PIB cft_PIB
*Butterworth
tsfilter bw bw_PIB = PIB, maxperiod(32) order(8) trend(bwt_PIB)
tsline PIB bwt_PIB
*Hoddrick y Prescott (datos diarios se recomienda lambda entre 10000 y 100000000)
tsfilter hp hp_PIB = PIB, smooth(1600) trend(hpt_PIB)
tsline PIB hpt_PIB
*Kalman
ucm PIB, model(rwdrift) cycle(1, frequency(.1)) cycle(1, frequency(3))
predict kalmant_PIB, trend
predict kalman1_PIB kalman2_PIB, cycle
gen kalman_PIB=kalman1_PIB+kalman2_PIB
*Creación logaritmos
foreach var in PIB bkt_PIB cft_PIB bwt_PIB hpt_PIB kalmant_PIB{
	gen ln`var'=ln(`var')
}
*GAPS
gen gap_bk=lnPIB-lnbkt_PIB
gen gap_cf=lnPIB-lncft_PIB
gen gap_bw=lnPIB-lnbwt_PIB
gen gap_hp=lnPIB-lnhpt_PIB
gen gap_kalman=lnPIB-lnkalmant_PIB
tsline gap_bk gap_cf gap_bw gap_hp gap_kalman

*Measures of outputgap
foreach var in bk cf bw hp kalman {
	gen abs_gap_`var'=abs(gap_`var')
	egen rev_`var' = sum(abs_gap_`var')
	replace rev_`var'=rev_`var'/_n
	gen inv_rev_`var'=1/rev_`var'
}

egen rev_total=rowtotal(rev_bk rev_cf rev_bw rev_hp rev_kalman)
egen inv_rev_total=rowtotal(inv_rev_bk inv_rev_cf inv_rev_bw inv_rev_hp inv_rev_kalman)

foreach var in bk cf bw hp kalman {
	gen weight_rev_`var'=rev_`var'/rev_total
	gen weight_inv_rev_`var'=inv_rev_`var'/inv_rev_total
}

gen weighted_rev_potential=weight_rev_bk[_n]*lnbkt_PIB+weight_rev_cf[_n]*lncft_PIB+ weight_rev_bw[_n]*lnbwt_PIB+weight_rev_hp[_n]*lnhpt_PIB+weight_rev_kalman[_n]*lnkalmant_PIB
gen weighted_inv_rev_potential=weight_inv_rev_bk[_n]*lnbkt_PIB+weight_inv_rev_cf[_n]*lncft_PIB+ weight_inv_rev_bw[_n]*lnbwt_PIB+weight_inv_rev_hp[_n]*lnhpt_PIB+weight_inv_rev_kalman[_n]*lnkalmant_PIB

gen weighted_rev_gap=lnPIB-weighted_rev_potential
gen weighted_inv_rev_gap=lnPIB-weighted_inv_rev_potential

export excel Year Quarter PIB Variation t kalmant_PIB kalman_PIB lnPIB lnkalmant_PIB gap_kalman using "Kalman_filter", firstrow(variables) replace