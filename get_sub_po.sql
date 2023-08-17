WITH all_po AS ( SELECT sub_po, SUM ( money_topup ) AS total_topup_for_subpo, min(card_po.transaction_created_at) as transaction_time FROM ub_holistics.card_po GROUP BY sub_po ) SELECT
sub_po,
CASE

    WHEN CHARINDEX ( '/', sub_po ) > 0 THEN
    urbox_sub_po_ver2."user"
    WHEN CHARINDEX ( '/', sub_po ) = 0 then 'mai.nt@urbox.vn'
  END AS nguoi_tao,
CASE

    WHEN  urbox_po_ver2.ngay_tao_po is not null and CHARINDEX ( '/', sub_po ) > 0 THEN cast ( urbox_po_ver2.ngay_tao_po AS DATE )
    WHEN CHARINDEX ( '/', sub_po ) = 0 then CAST ( "public".convert_timezone ( customer_order.created_at ) AS DATE )
    when urbox_po_ver2.ngay_tao_po is null then transaction_time
  END AS ngay_khai_bao_po,
  total_topup_for_subpo

FROM
  all_po
  LEFT JOIN ub_rawdata.urbox_sub_po_ver2 on all_po.sub_po = urbox_sub_po_ver2.sub_po_code
  left join ub_rawdata.urbox_po_ver2 ON urbox_sub_po_ver2.po_id = urbox_po_ver2.id_po
  left join urco.customer_order on all_po.sub_po = customer_order.co_code