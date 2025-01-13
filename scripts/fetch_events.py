import os
import json
import pandas as pd
from datetime import datetime
from serpapi import GoogleSearch

def flatten_address(address_list):
    """Flatten address list to string"""
    if isinstance(address_list, list):
        return ', '.join(address_list)
    return address_list

def extract_ticket_info(ticket_info):
    """Extract ticket information into a structured format"""
    if not ticket_info:
        return None
    tickets = []
    for ticket in ticket_info:
        tickets.append(f"{ticket.get('source', '')}: {ticket.get('link', '')}")
    return '; '.join(tickets)

def fetch_events(city, api_key=None):
    """Fetch events from Google Events API and save as CSV"""
    if not api_key:
        api_key = os.getenv('SERPAPI_KEY')
    
    if not api_key:
        raise ValueError("API key is required either as parameter or SERPAPI_KEY environment variable")
    
    params = {
        "engine": "google_events",
        "q": f"Events in {city}",
        "hl": "en",
        "gl": "us",
        "api_key": api_key
    }
    
    try:
        # Fetch events from API
        search = GoogleSearch(params)
        results = search.get_dict()
        events_results = results.get("events_results", [])
        
        if not events_results:
            print("No events found")
            return None
            
        # Process events data
        processed_events = []
        for event in events_results:
            processed_event = {
                'title': event.get('title'),
                'date': event.get('date', {}).get('start_date'),
                'when': event.get('date', {}).get('when'),
                'address': flatten_address(event.get('address')),
                'description': event.get('description'),
                'link': event.get('link'),
                'venue_name': event.get('venue', {}).get('name'),
                'venue_rating': event.get('venue', {}).get('rating'),
                'venue_reviews': event.get('venue', {}).get('reviews'),
                'thumbnail': event.get('thumbnail'),
                'ticket_info': extract_ticket_info(event.get('ticket_info')),
                'fetch_date': datetime.now().strftime('%Y-%m-%d')
            }
            processed_events.append(processed_event)
        
        # Convert to DataFrame
        df = pd.DataFrame(processed_events)
        
        # Save as CSV
        output_path = f"/opt/airflow/data/raw/events_{city.lower()}_{datetime.now().strftime('%Y%m%d')}.csv"
        os.makedirs(os.path.dirname(output_path), exist_ok=True)
        df.to_csv(output_path, index=False)
        
        print(f"Successfully saved {len(processed_events)} events to {output_path}")
        return output_path
        
    except Exception as e:
        print(f"Error fetching events: {str(e)}")
        raise

if __name__ == "__main__":
    city = "Austin"
    fetch_events(city)