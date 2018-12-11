import axios from 'axios'

const API_URL = 'http://localhost:11181'

export function fetchTwitterRequests () {
  return axios.get(`${API_URL}/api/twitter/requests`, {})
}
