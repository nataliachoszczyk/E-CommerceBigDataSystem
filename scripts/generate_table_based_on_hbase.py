#!/usr/bin/env python3
"""
Agreguje dane z HBase dla pojedynczego okna czasowego.
Obsługuje sliding windows - dla każdej marki/produktu wybiera najnowszy snapshot,
następnie sortuje po faktycznej liczbie views/events.
"""

import sys
import happybase
from collections import defaultdict

def get_window_summary(window_key='20191230_2111'):
    """Pobiera i agreguje dane dla danego okna czasowego."""
    
    conn = happybase.Connection('localhost', 9090)
    table = conn.table('realtime_stats')
    
    rows = list(table.scan(
        row_start=window_key.encode(),
        row_stop=f"{window_key}~".encode()
    ))
    
    print(f"Pobrano {len(rows)} wierszy z HBase dla okna {window_key}")
    
    stats = {
        'active_users': {'users': 0, 'events': 0},
        'purchases': {'count': 0, 'revenue': 0, 'avg': 0, 'buyers': 0},
        'events': {'view': 0, 'cart': 0, 'purchase': 0}
    }

    brands_snapshots = {}
    products_snapshots = {}
    
    for key, data in rows:
        key_str = key.decode()
        
        # === ACTIVE USERS ===
        if 'active_users' in key_str:
            stats['active_users']['users'] = int(data.get(b'metrics:active_users', 0))
            stats['active_users']['events'] = int(data.get(b'metrics:total_events', 0))
        
        # === PURCHASES ===
        elif 'purchases' in key_str and 'event' not in key_str:
            stats['purchases']['count'] = int(data.get(b'metrics:purchase_count', 0))
            stats['purchases']['revenue'] = float(data.get(b'metrics:total_revenue', b'0'))
            stats['purchases']['avg'] = float(data.get(b'metrics:avg_basket_value', b'0'))
            stats['purchases']['buyers'] = int(data.get(b'metrics:purchasing_users', 0))
        
        # === EVENT DISTRIBUTION ===
        elif 'event_view' in key_str:
            stats['events']['view'] = int(data.get(b'metrics:event_count', 0))
        elif 'event_cart' in key_str:
            stats['events']['cart'] = int(data.get(b'metrics:event_count', 0))
        elif 'event_purchase' in key_str:
            stats['events']['purchase'] = int(data.get(b'metrics:event_count', 0))
        
        # === TOP BRANDS (z deduplikacją) ===
        elif 'brand_' in key_str:
            brand = data.get(b'metrics:brand', b'').decode()
            event_type = data.get(b'metrics:event_type', b'').decode()
            rank = int(data.get(b'metrics:rank', 0))
            count = int(data.get(b'metrics:event_count', 0))
            value = float(data.get(b'metrics:total_value', b'0'))
            created_at = data.get(b'metadata:created_at', b'').decode()
            
            unique_key = f"{brand}_{event_type}"
            
            if unique_key not in brands_snapshots or created_at > brands_snapshots[unique_key]['created_at']:
                brands_snapshots[unique_key] = {
                    'brand': brand,
                    'event_type': event_type,
                    'rank': rank,
                    'count': count,
                    'value': value,
                    'created_at': created_at
                }
        
        # === TOP PRODUCTS (z deduplikacją) ===
        elif 'top_view' in key_str:
            product_id = data.get(b'metrics:product_id', b'').decode()
            brand = data.get(b'metrics:brand', b'').decode()
            rank = int(data.get(b'metrics:rank', 0))
            count = int(data.get(b'metrics:view_count', 0))
            price = float(data.get(b'metrics:avg_price', b'0'))
            category = data.get(b'metrics:category', b'').decode()
            created_at = data.get(b'metadata:created_at', b'').decode()
            
            unique_key = product_id
            
            if unique_key not in products_snapshots or created_at > products_snapshots[unique_key]['created_at']:
                products_snapshots[unique_key] = {
                    'product_id': product_id,
                    'brand': brand,
                    'rank': rank,
                    'count': count,
                    'price': price,
                    'category': category,
                    'created_at': created_at
                }
    
    conn.close()

    top_brands = sorted(brands_snapshots.values(), key=lambda x: -x['count'])[:10]
    top_products = sorted(products_snapshots.values(), key=lambda x: -x['count'])[:10]
    
    for i, brand in enumerate(top_brands, 1):
        brand['actual_rank'] = i
    for i, product in enumerate(top_products, 1):
        product['actual_rank'] = i
    
    return stats, top_brands, top_products, len(rows)


def print_summary(window_key, stats, top_brands, top_products, total_rows):
    """Wyświetla podsumowanie w czytelnej formie."""
    
    print(f"\n{'='*80}")
    print(f"OKNO CZASOWE: {window_key}")
    print(f"{'='*80}\n")
    
    # === ACTIVE USERS ===
    print("┌─────────────────────────────────────────────────────────────────┐")
    print("│ ACTIVE USERS                                                    │")
    print("├─────────────────────────────────────────────────────────────────┤")
    users = stats['active_users']['users']
    events = stats['active_users']['events']
    avg = events / users if users > 0 else 0
    print(f"│ • {users:,} unique users                                     │".ljust(66) + "│")
    print(f"│ • {events:,} total events                                    │".ljust(66) + "│")
    print(f"│ • {avg:.1f} events/user avg                                  │".ljust(66) + "│")
    print("└─────────────────────────────────────────────────────────────────┘\n")
    
    # === PURCHASES ===
    print("┌─────────────────────────────────────────────────────────────────┐")
    print("│ PURCHASES                                                       │")
    print("├─────────────────────────────────────────────────────────────────┤")
    count = stats['purchases']['count']
    revenue = stats['purchases']['revenue']
    avg_basket = stats['purchases']['avg']
    buyers = stats['purchases']['buyers']
    print(f"│ • {count} transactions                                        │".ljust(66) + "│")
    print(f"│ • ${revenue:,.2f} total revenue                             │".ljust(66) + "│")
    print(f"│ • ${avg_basket:.2f} avg basket value                         │".ljust(66) + "│")
    print(f"│ • {buyers} unique buyers                                      │".ljust(66) + "│")
    print("└─────────────────────────────────────────────────────────────────┘\n")
    
    # === EVENT DISTRIBUTION ===
    print("┌─────────────────────────────────────────────────────────────────┐")
    print("│ EVENT DISTRIBUTION                                              │")
    print("├─────────────────────────────────────────────────────────────────┤")
    view_count = stats['events']['view']
    cart_count = stats['events']['cart']
    purchase_count = stats['events']['purchase']
    view_to_cart = (cart_count / view_count * 100) if view_count > 0 else 0
    cart_to_purchase = (purchase_count / cart_count * 100) if cart_count > 0 else 0
    print(f"│ • {view_count:,} views                                       │".ljust(66) + "│")
    print(f"│ • {cart_count:,} carts                                       │".ljust(66) + "│")
    print(f"│ • {purchase_count:,} purchases                                │".ljust(66) + "│")
    print(f"│ • Conversion: {view_to_cart:.1f}% (view→cart)                │".ljust(66) + "│")
    print(f"│ • Conversion: {cart_to_purchase:.1f}% (cart→purchase)        │".ljust(66) + "│")
    print("└─────────────────────────────────────────────────────────────────┘\n")
    
    # === TOP PRODUCTS ===
    print("┌─────────────────────────────────────────────────────────────────┐")
    print("│ TOP PRODUCTS (top 10 by views - sorted by count)               │")
    print("├─────────────────────────────────────────────────────────────────┤")
    for product in top_products[:10]:
        rank = product.get('actual_rank', product['rank'])  
        brand = product['brand'] or 'unknown'
        pid = product['product_id']
        count = product['count']
        price = product['price']
        print(f"│ • Rank #{rank:02d}: {brand} {pid} ({count} views, ${price:.2f})│".ljust(66) + "│")
    print("└─────────────────────────────────────────────────────────────────┘\n")
    
    # === TOP BRANDS ===
    print("┌─────────────────────────────────────────────────────────────────┐")
    print("│ TOP BRANDS (top 10 by event count - sorted by count)           │")
    print("├─────────────────────────────────────────────────────────────────┤")
    for brand_data in top_brands[:10]:
        rank = brand_data.get('actual_rank', brand_data['rank'])  
        brand = brand_data['brand']
        event_type = brand_data['event_type']
        count = brand_data['count']
        value = brand_data['value']
        print(f"│ • Rank #{rank:02d}: {brand} [{event_type}] ({count} events, ${value:,.2f})│".ljust(66) + "│")
    print("└─────────────────────────────────────────────────────────────────┘\n")
    
    # === METADATA ===
    unique_brands = len(set(b['brand'] for b in top_brands))
    unique_products = len(top_products)  
    
    print(f"{'='*80}")
    print(f"METADATA:")
    print(f"  • Total rows in HBase: {total_rows}")
    print(f"  • Unique brands (after deduplication): {unique_brands}")
    print(f"  • Unique products (after deduplication): {unique_products}")
    print(f"  • Note: Multiple rows per brand/product due to sliding windows")
    print(f"{'='*80}\n")


if __name__ == "__main__":
    window_key = sys.argv[1] if len(sys.argv) > 1 else '20191230_2111'
    
    try:
        stats, top_brands, top_products, total_rows = get_window_summary(window_key)

        print_summary(window_key, stats, top_brands, top_products, total_rows)
        
    except Exception as e:
        print(f"❌ Błąd: {e}", file=sys.stderr)
        import traceback
        traceback.print_exc()
        sys.exit(1)
