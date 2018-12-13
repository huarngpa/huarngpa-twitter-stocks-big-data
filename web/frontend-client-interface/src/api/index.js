import axios from 'axios'

const API_URL = 'http://localhost:11181'

export function fetchTwitterRequests () {
  return axios.get(`${API_URL}/api/twitter/requests`, {})
}

export function fetchStockRequests () {
  return axios.get(`${API_URL}/api/stock/requests`, {})
}

export function makeTwitterRequest (twitterUser) {
  return axios.get(`${API_URL}/api/twitter/${twitterUser}`, {})
}

export function makeStockRequest (stockTicker) {
  return axios.get(`${API_URL}/api/stock/${stockTicker}`, {})
}

export function fetchTwitterWeekly () {
  return axios.get(`${API_URL}/api/twitter/weekly`, {})
}

export function fetchStockWeekly () {
  return axios.get(`${API_URL}/api/stock/weekly`, {})
}
