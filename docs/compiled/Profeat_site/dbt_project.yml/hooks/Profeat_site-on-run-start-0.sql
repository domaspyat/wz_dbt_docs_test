
CREATE OR REPLACE FUNCTION dbt_nbespalov.good_balance(x array<FLOAT64>, transaction_type array<string>, subscription_update_id array<string>)
  returns array<struct<good_balance_spent float64, subscription_update_id string>>
  LANGUAGE js AS """
  output = []; good_balance_current=0;
  for(i = 0; i < x.length; i++){
    a = {};
    if (transaction_type[i]  == 'good_balance') {
      good_balance_current+=x[i]
      a.good_balance=good_balance_current
      a.good_balance_spent=0
      output.push(a)
  }
   else if (transaction_type[i]  == 'bad_balance') {
      a.good_balance=good_balance_current
      a.good_balance_spent=0
      a.transaction_type=transaction_type[i]
      output.push(a)
  } 
 else if (transaction_type[i]  == 'subscription') {
  if (good_balance_current>0 & x[i]<=good_balance_current){
      good_balance_current -= x[i]
      a.good_balance_spent=x[i]
      a.subscription_update_id=subscription_update_id[i]
      output.push(a)
  }
 else if (good_balance_current>0 & x[i]>=good_balance_current){
      a.good_balance_spent=good_balance_current
      a.subscription_update_id=subscription_update_id[i]
      good_balance_current = 0
      
      output.push(a)
  }
 }
  else {
     a.good_balance=good_balance_current
     a.subscription_update_id=subscription_update_id[i]
     output.push(a)
  }

}

  
  return output;
""";

