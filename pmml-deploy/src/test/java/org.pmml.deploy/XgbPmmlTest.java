package org.pmml.deploy;

/**
 * Created by yihaibo on 2019-06-13.
 */

import org.dmg.pmml.FieldName;
import org.dmg.pmml.PMML;
import org.jpmml.evaluator.*;

import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public class XgbPmmlTest {
    public static void main(String[] args) throws Exception {
        String  pathxml="aa_card_1.pmml";

        Map<String, Double>  map=new HashMap<String, Double>();
        map.put("last_6_month_category_weekend_entertainment_orders",7.0);
        map.put("last_1_year_category_workday_placename_orders",0.0);
        map.put("last_6_month_category_workday_placename_orders",0.0);
        map.put("last_1_year_fast_weixin_finish_order_rate",0.0);
        map.put("card_prov_area_6",1.0);
        map.put("last_6_month_gulf_night_call_orders",50.0);
        map.put("last_1_year_invoice_value",null);
        map.put("last_30_days_wifi_number",13.0);
        map.put("card_prov_area_8",0.0);
        map.put("last_1_year_avg_normal_distance",10.7061);
        map.put("last_6_month_gulf_weekend_finish_orders",39.0);
        map.put("last_1_year_category_placename_rodename_orders",null);
        map.put("last_6_month_fast_morning_finish_order_rate",0.35593220339);
        map.put("last_6_month_min_gulf_late_time",0.08);
        map.put("last_30_day_gulf_app_finish_orders",15.0);
        map.put("last_14_day_fast_morning_finish_order_rate",0.0);
        map.put("last_30_day_gulf_wx_pay_actual_cost_rate",1.0);
        map.put("last_6_month_category_weekend_company_orders",0.0);
        map.put("last_1_year_fast_morning_finish_orders_rate",0.260869565217);
        map.put("last_1_year_fast_carpool_finish_order_actual_cost",767.5);
        map.put("last_3_month_invoice_orders",null);
        map.put("last_30_day_avg_fast_late_time",null);
        map.put("last_6_month_fast_ali_pay_actual_cost_rate",0.0);
        map.put("last_6_month_category_workday_delicacy_orders",9.0);
        map.put("app_system_tools_wifi_category_number_rate",null);
        map.put("last_6_month_category_weekend_lifeservice_orders",1.0);
        map.put("last_3_month_night_finish_rate",0.0645);
        map.put("last_1_year_fast_ali_pay_actual_cost_rate",0.0);
        map.put("last_1_year_category_entertainment_outdooract_orders",0.0);
        map.put("last_6_month_fast_weixin_finish_orders_rate",0.0);
        map.put("order_starting_stability",0.1793);
        map.put("last_6_month_category_infrastructure_transfacilities_orders",4.0);
        map.put("last_1_year_other_city_finish_orders_rate",0.0623);
        map.put("last_6_month_taxi_night_call_orders",1.0);
        map.put("pas_age",28.0);
        map.put("last_3_month_category_weekend_company_orders",0.0);
        map.put("last_30_day_sum_normal_distance",203.63);
        map.put("last_1_year_fast_carpool_call_orders_rate",0.237762237762);
        map.put("lic_audit_status",0.0);
        map.put("last_30_day_fast_carpool_pas_ord_dis",22.17);
        map.put("last_1_year_category_workday_infrastructure_orders",3.0);
        map.put("last_3_month_bike_ride_distance",null);
        map.put("last_6_month_category_placename_orders",null);
        map.put("last_6_month_fast_night_finish_rate",0.186440677966);
        map.put("last_3_month_two_star_orders",0.0);
        map.put("last_6_month_invoice_value",null);
        map.put("last_1_month_category_infrastructure_transfacilities_orders",1.0);
        map.put("last_3_month_fast_late_time",1.1);
        map.put("last_30_days_invoice_value",null);
        map.put("last_3_month_fast_app_finish_orders",13.0);
        map.put("last_6_month_taxi_wx_pay_actual_cost",0.0);
        map.put("last_6_month_night_finish_rate",0.0979);
        map.put("last_3_month_fast_ali_pay_actual_cost",0.0);
        map.put("last_3_month_taxi_finish_order_days",1.0);
        map.put("last_3_month_taxi_finish_order_actual_cost",0.0);
        map.put("finish_count_stability",0.175);
        map.put("last_6_month_fast_night_finish_orders",11.0);
        map.put("last_1_year_category_infrastructure_transfacilities_orders",5.0);
        map.put("last_6_month_bike_ride_duration",null);
        map.put("last_6_month_fast_late_time",17.96);
        map.put("house_in_city_rk",1700.77419355);
        map.put("last_1_year_fast_agent_cost_rate",0.737576586703);
        map.put("last_1_year_taxi_wx_pay_actual_cost",0.0);
        map.put("last_1_year_category_workday_hotel_orders",13.0);
        map.put("last_14_day_fast_pas_ord_dis",43.77);
        map.put("last_6_month_fast_agent_cost_rate",0.650536299858);
        map.put("last_6_month_fast_wx_pay_actual_cost",2296.29);
        map.put("last_30_day_min_fast_late_time",null);
        map.put("last_30_day_fast_finish_order_actual_cost",66.8);
        map.put("pas_reg_days_greater_2year",1.0);
        map.put("last_30_day_airport_finish_payable_cost_rate",0.2208);
        map.put("last_1_year_bike_ride_duration",null);
        map.put("last_6_month_fast_wx_pay_actual_cost_rate",1.0);
        map.put("last_3_month_category_weekend_estate_orders",6.0);
        map.put("last_30_day_bike_ride_distance",null);
        map.put("last_1_year_category_entertainment_teahouse_orders",0.0);
        map.put("last_1_year_fast_finish_order_actual_cost",4261.51);
        map.put("last_1_year_category_entertainment_orders",18.0);
        map.put("last_1_year_driver_complaint_orders",1.0);
        map.put("last_1_year_taxi_finish_order_actual_cost",154.0);
        map.put("last_1_year_fast_late_time",72.4);
        map.put("last_1_year_category_entertainment_concert_orders",2.0);
        map.put("last_1_year_category_weekend_school_orders",1.0);
        map.put("last_1_year_fast_night_finish_rate",0.208695652174);
        map.put("app_business_note_category_number_rate",null);
        map.put("last_3_month_avg_normal_distance",8.1805);
        map.put("last_6_month_category_estate_businessbuild_orders",50.0);
        map.put("app_loan_sub_category_number_rate",null);
        map.put("5_month_taxi_finish_orders",0.0);
        map.put("consume_value_score",48.4);
        map.put("last_1_year_category_venue_exhibitioncente_orders",2.0);
        map.put("last_1_year_gulf_app_finish_orders_rate",1.0);
        map.put("last_1_year_night_finish_rate",0.0856);
        map.put("last_1_year_category_workday_entertainment_orders",10.0);
        map.put("app_loan_sub_category_number",null);
        map.put("last_3_month_fast_carpool_finish_orders_rate",0.714285714286);
        map.put("app_business_category_number",null);
        map.put("pas_reg_days_greater_3year",1.0);
        map.put("first_gulf_call_days_greater_1year",1.0);
        map.put("last_1_year_bike_orders",null);
        map.put("last_1_year_night_finish_count",22.0);
        map.put("last_7_day_avg_fast_late_time",null);
        map.put("last_1_year_fast_night_call_orders",50.0);
        map.put("last_30_day_min_start_times",14.0);
        map.put("consume_stability",0.1944);
        map.put("last_1_year_category_workday_delicacy_orders",12.0);
        map.put("last_gulf_call_days",6.0);
        map.put("last_6_month_category_addfacilities_orders",33.0);
        map.put("last_30_day_fast_pas_ord_dis",43.77);
        map.put("last_1_year_category_weekend_company_orders",1.0);
        map.put("last_3_month_category_entertainment_orders",7.0);
        map.put("app_financial_category_number_rate",null);
        map.put("app_health_category_number",null);
        map.put("last_6_month_fast_finish_order_max_actual_cost",158.71);
        map.put("app_pay_sub_category_number_rate",null);
        map.put("last_1_year_category_shopping_unionmarket_orders",6.0);
        map.put("last_1_year_category_workday_company_orders",5.0);
        map.put("last_6_month_category_school_middleschool_orders",0.0);
        map.put("last_7_day_min_fast_late_time",null);
        map.put("last_1_year_fast_ali_pay_actual_cost",0.0);
        map.put("last_7_day_fast_carpool_finish_order_payable_cost",0.0);
        map.put("last_3_month_category_workday_company_orders",2.0);
        map.put("avg_community_price",90300.9249347);
        map.put("app_stock_sub_category_number_rate",null);
        map.put("last_1_year_fast_morning_finish_orders",30.0);
        map.put("last_1_month_category_delicacy_orders",1.0);
        map.put("last_3_month_max_gulf_late_time",43.27);
        map.put("app_life_category_number",null);
        map.put("last_1_month_category_shopping_appliancestore_orders",0.0);
        map.put("first_call_days_greater_1year",1.0);
        map.put("last_6_month_invoice_count",null);
        map.put("last_30_day_fast_ali_pay_actual_cost",0.0);
        map.put("last_1_year_fast_night_finish_orders",24.0);
        map.put("last_6_month_finish_count_variation_coefficient",0.25);
        map.put("last_6_month_reserve_finish_rate",0.0103);
        predictLrHeart(map, pathxml);
    }

    public static void predictLrHeart(Map<String, Double> kxmap, String  pathxml)throws Exception {

        PMML pmml;
        File file = new File(XgbPmmlTest.class.getClassLoader().getResource(pathxml).getFile());
        InputStream inputStream = new FileInputStream(file);
        try (InputStream is = inputStream) {
            pmml = org.jpmml.model.PMMLUtil.unmarshal(is);

            ModelEvaluatorFactory modelEvaluatorFactory = ModelEvaluatorFactory
                    .newInstance();
            ModelEvaluator<?> modelEvaluator = modelEvaluatorFactory
                    .newModelEvaluator(pmml);
            Evaluator evaluator = (Evaluator) modelEvaluator;

            List<InputField> inputFields = evaluator.getInputFields();

            Map<FieldName, FieldValue> arguments = new LinkedHashMap<FieldName, FieldValue>();
            for (InputField inputField : inputFields) {
                FieldName inputFieldName = inputField.getName();
                Object rawValue = kxmap
                        .get(inputFieldName.getValue());
                FieldValue inputFieldValue = inputField.prepare(rawValue);
                arguments.put(inputFieldName, inputFieldValue);
            }

            Map<FieldName, ?> results = evaluator.evaluate(arguments);
            List<TargetField> targetFields = evaluator.getTargetFields();

            for (TargetField targetField : targetFields) {
                FieldName targetFieldName = targetField.getName();
                Object targetFieldValue = results.get(targetFieldName);
                System.out.println("target: " + targetFieldName.getValue()
                        + " value: " + targetFieldValue);
            }
        }catch (Exception e) {
            inputStream.close();
        }
    }
}
