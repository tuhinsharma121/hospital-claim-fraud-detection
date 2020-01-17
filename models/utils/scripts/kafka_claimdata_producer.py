import json
import random
import time
from datetime import datetime

from kafka import KafkaProducer

kafka_host = "localhost"
topic = "claim_event_topic"

patient_names_list = ['Esco Boehm', 'Arnold Lowe', 'Kiara Reinger', 'Armin Sporer', 'Fatima Rutherford',
                      'Christy Ortiz', 'Coraima Prosacco', 'Haiden Shields', 'Janel Greenholt', 'Orlena Ziemann',
                      'Renata Waters', 'Bowman Bernier', 'Cailyn Dicki', 'Zigmund Bednar', 'Ocie Ebert', 'Kunta Hickle',
                      'Siena Block', 'Alta Pfannerstill', 'Gianni Hansen', 'Jerrod Zulauf', 'Marlo Aufderhar',
                      'Tia Durgan', 'Suzanne Rempel', 'Eloise Senger', 'Latoyia Hegmann', 'Conard Lueilwitz',
                      'Kendal Pagac', 'Refugio Breitenberg', 'Vivien Yundt', 'Chester Kessler', 'Purl Fahey',
                      'Deja Schowalter', 'Champ Simonis', 'Demetria Feeney', 'Ashtyn Fadel', 'Giuseppe Sanford',
                      'Katina Schamberger', 'Delinda Mante', 'Ingrid Lubowitz', 'Alba Beatty', 'Vallie Conn',
                      'Rubye Denesik', 'Margurite Brakus', 'Leonard Gulgowski', 'Kermit Collins', 'Johny Schiller',
                      'Jonas Turner', 'Jamar Turcotte', 'Thorwald Heller', 'Monroe Wuckert', 'Fitzgerald Walker',
                      'Dilan Larson', 'Arianna Lang', 'Irena Robel', 'Ethelyn Beer', 'Jayleen Swaniawski',
                      'Casimer Nitzsche', 'Mable Schulist', 'Jacque Frami', 'Madelyn Schuster', 'Tai Moen',
                      'Edris Osinski', 'Demarco Daugherty', 'Jayme Schumm', 'Lela Langworth', "Jannette O'Reilly",
                      'Nile Johnson', 'Nyah Raynor', 'Zana Green', 'Percival Kiehn', 'Ellis Anderson',
                      'Keyshawn Cremin', 'Stephenie Olson', "Reno O'Kon", 'Boyce Hyatt', 'Claudius Erdman',
                      'Harlon Morissette', 'Debroah Jenkins', 'Beecher Little', 'Izetta Schimmel', 'Jiles West',
                      'Bethel Hermiston', 'Hubert Kuhn', 'Gustav McLaughlin', 'Agustus Tillman', 'Auther Schultz',
                      'Samuel Ruecker', 'Neppie Hettinger', 'Doctor Hermann', 'Medora Spinka', 'Saniya Bins',
                      'Bronson Dickinson', 'Dewayne Kuphal', 'Romona Hackett', 'Ashanti Wintheiser', 'Nikolai Douglas',
                      'Deanne Mohr', 'Alpha Harvey', 'Daren Corwin', 'Princess Collier', 'Kamryn Bode', 'Peter Haag',
                      'Shannan Goyette', 'Octave Grady', 'Verda Cole', 'Floy Sauer', 'Maximo Casper', 'Tobin Hirthe',
                      'Malorie Gerlach', 'Lissette Armstrong', 'Zillah Hamill', 'Lucky Trantow', 'Fred Mayer',
                      'Cathy Ledner', "Elizbeth O'Connell", 'Ona Strosin', 'Polly Smitham', 'Erlinda Legros',
                      'Edie Ankunding', 'Carin Bechtelar', 'Anabella Gleichner', 'Nikhil Muller', 'Arturo Grant',
                      'Denis Hills', 'Alexande Kulas', 'Asher Mueller', 'Lucy Stokes', 'Ian Auer', 'Tangela Schmidt',
                      'Anabelle Halvorson', 'Yesenia Farrell', 'Rexford Jast', 'Odalys Boyle', 'Louvenia Quigley',
                      'Jody Botsford', 'Carlyle Sipes', 'Elenora McKenzie', 'Dexter Jerde', 'Colton Jacobi',
                      'Lilianna Bogan', 'Audriana Kovacek', 'Bartley Hayes', 'Brandin Mann', 'Regis Cartwright',
                      'Pauline Torphy', 'Iza Herzog', 'Leilani Kuhlman', 'Monica Lebsack', 'Casey Kreiger',
                      'Bryant Spencer', 'Georgene Torp', 'Gene Runte', 'Melissa Koch', 'Micayla Ondricka',
                      'Todd Harber', 'Wilbur Rau', 'Yair Koepp', 'Brooklynn Kub', 'Drew Schroeder', 'Michial Stanton',
                      'Florencio Walsh', 'Hayden Christiansen', 'Christine Baumbach', 'Minna Macejkovic',
                      'Devante Powlowski', 'Gonzalo Borer', 'Tiny Stracke', 'Ab Kshlerin', 'Kalene Kling',
                      'Mossie Hessel', 'Mathews Swift', 'Mae Rolfson', 'Brody Schaefer', 'Tyreek Jaskolski',
                      'Abram Nienow', 'Delilah Russel', 'Opha Kohler', 'Versie Stamm', 'Yareli Bailey',
                      'Madelynn Parisian', 'Iyana Herman', 'Needham Schaden', 'Tatyanna Beahan', 'Rennie Glover',
                      'Marcell Dare', 'Malvin Berge', 'Ely Roberts', 'Laurie Runolfsdottir', 'Letta Kautzer',
                      'Plummer Kertzmann', 'Norita Bradtke', 'Posey Bartoletti', 'Ulysses White', 'Tyler Gleason',
                      'Zaida Becker', 'Graydon Skiles', 'Danelle Nolan', 'Angie Dibbert', 'Theodocia Smith',
                      'Jaycie Larkin']

patient_id_list = ['PAT1', 'PAT2', 'PAT3', 'PAT4', 'PAT5', 'PAT6', 'PAT7', 'PAT8', 'PAT9', 'PAT10', 'PAT11', 'PAT12',
                   'PAT13', 'PAT14', 'PAT15', 'PAT16', 'PAT17', 'PAT18', 'PAT19', 'PAT20', 'PAT21', 'PAT22', 'PAT23',
                   'PAT24', 'PAT25', 'PAT26', 'PAT27', 'PAT28', 'PAT29', 'PAT30', 'PAT31', 'PAT32', 'PAT33', 'PAT34',
                   'PAT35', 'PAT36', 'PAT37', 'PAT38', 'PAT39', 'PAT40', 'PAT41', 'PAT42', 'PAT43', 'PAT44', 'PAT45',
                   'PAT46', 'PAT47', 'PAT48', 'PAT49', 'PAT50', 'PAT51', 'PAT52', 'PAT53', 'PAT54', 'PAT55', 'PAT56',
                   'PAT57', 'PAT58', 'PAT59', 'PAT60', 'PAT61', 'PAT62', 'PAT63', 'PAT64', 'PAT65', 'PAT66', 'PAT67',
                   'PAT68', 'PAT69', 'PAT70', 'PAT71', 'PAT72', 'PAT73', 'PAT74', 'PAT75', 'PAT76', 'PAT77', 'PAT78',
                   'PAT79', 'PAT80', 'PAT81', 'PAT82', 'PAT83', 'PAT84', 'PAT85', 'PAT86', 'PAT87', 'PAT88', 'PAT89',
                   'PAT90', 'PAT91', 'PAT92', 'PAT93', 'PAT94', 'PAT95', 'PAT96', 'PAT97', 'PAT98', 'PAT99', 'PAT100',
                   'PAT101', 'PAT102', 'PAT103', 'PAT104', 'PAT105', 'PAT106', 'PAT107', 'PAT108', 'PAT109', 'PAT110',
                   'PAT111', 'PAT112', 'PAT113', 'PAT114', 'PAT115', 'PAT116', 'PAT117', 'PAT118', 'PAT119', 'PAT120',
                   'PAT121', 'PAT122', 'PAT123', 'PAT124', 'PAT125', 'PAT126', 'PAT127', 'PAT128', 'PAT129', 'PAT130',
                   'PAT131', 'PAT132', 'PAT133', 'PAT134', 'PAT135', 'PAT136', 'PAT137', 'PAT138', 'PAT139', 'PAT140',
                   'PAT141', 'PAT142', 'PAT143', 'PAT144', 'PAT145', 'PAT146', 'PAT147', 'PAT148', 'PAT149', 'PAT150',
                   'PAT151', 'PAT152', 'PAT153', 'PAT154', 'PAT155', 'PAT156', 'PAT157', 'PAT158', 'PAT159', 'PAT160',
                   'PAT161', 'PAT162', 'PAT163', 'PAT164', 'PAT165', 'PAT166', 'PAT167', 'PAT168', 'PAT169', 'PAT170',
                   'PAT171', 'PAT172', 'PAT173', 'PAT174', 'PAT175', 'PAT176', 'PAT177', 'PAT178', 'PAT179', 'PAT180',
                   'PAT181', 'PAT182', 'PAT183', 'PAT184', 'PAT185', 'PAT186', 'PAT187', 'PAT188', 'PAT189', 'PAT190',
                   'PAT191', 'PAT192', 'PAT193', 'PAT194', 'PAT195', 'PAT196', 'PAT197', 'PAT198', 'PAT199', 'PAT200']

patient_age_list = [47, 20, 67, 32, 56, 42, 41, 37, 49, 54, 32, 28, 40, 73, 23, 26, 22, 17, 69, 34, 27, 19, 36, 19, 74,
                    56, 62, 51, 18, 68, 19, 21, 29, 68, 38, 56, 62, 37, 39, 72, 66, 56, 27, 73, 26, 23, 62, 23, 38, 49,
                    16, 51, 25, 64, 59, 27, 54, 15, 46, 31, 37, 19, 18, 35, 22, 62, 58, 30, 23, 35, 16, 20, 70, 30, 21,
                    71, 16, 41, 21, 29, 66, 75, 38, 30, 75, 70, 28, 26, 25, 21, 52, 36, 48, 50, 30, 49, 47, 75, 30, 41,
                    57, 57, 50, 58, 17, 68, 62, 46, 72, 30, 35, 63, 37, 45, 44, 54, 24, 65, 55, 74, 42, 66, 70, 69, 18,
                    26, 41, 43, 43, 41, 41, 31, 51, 19, 33, 36, 68, 22, 75, 50, 71, 61, 24, 34, 61, 35, 53, 44, 32, 49,
                    75, 58, 74, 33, 17, 61, 18, 47, 58, 31, 15, 69, 56, 29, 52, 42, 60, 72, 24, 23, 71, 62, 66, 28, 36,
                    25, 58, 23, 44, 21, 61, 56, 62, 31, 64, 24, 23, 36, 43, 27, 23, 70, 16, 28, 57, 21, 59, 58, 28, 35]

patient_gender_list = ['MALE', 'FEMALE', 'MALE', 'MALE', 'MALE', 'FEMALE', 'MALE', 'MALE', 'MALE', 'MALE', 'FEMALE',
                       'MALE', 'MALE', 'FEMALE', 'MALE', 'MALE', 'FEMALE', 'MALE', 'MALE', 'MALE', 'MALE', 'MALE',
                       'MALE', 'FEMALE', 'MALE', 'FEMALE', 'FEMALE', 'MALE', 'MALE', 'MALE', 'MALE', 'MALE', 'MALE',
                       'MALE', 'FEMALE', 'FEMALE', 'MALE', 'FEMALE', 'FEMALE', 'FEMALE', 'MALE', 'MALE', 'MALE', 'MALE',
                       'FEMALE', 'MALE', 'MALE', 'MALE', 'MALE', 'FEMALE', 'FEMALE', 'MALE', 'FEMALE', 'FEMALE', 'MALE',
                       'FEMALE', 'MALE', 'FEMALE', 'FEMALE', 'MALE', 'MALE', 'MALE', 'MALE', 'MALE', 'MALE', 'MALE',
                       'MALE', 'MALE', 'MALE', 'MALE', 'MALE', 'MALE', 'MALE', 'FEMALE', 'FEMALE', 'MALE', 'MALE',
                       'FEMALE', 'FEMALE', 'FEMALE', 'MALE', 'FEMALE', 'MALE', 'MALE', 'FEMALE', 'MALE', 'FEMALE',
                       'FEMALE', 'FEMALE', 'MALE', 'FEMALE', 'FEMALE', 'MALE', 'FEMALE', 'MALE', 'FEMALE', 'MALE',
                       'FEMALE', 'FEMALE', 'MALE', 'FEMALE', 'FEMALE', 'FEMALE', 'MALE', 'MALE', 'MALE', 'FEMALE',
                       'MALE', 'FEMALE', 'MALE', 'MALE', 'FEMALE', 'MALE', 'MALE', 'MALE', 'MALE', 'MALE', 'MALE',
                       'FEMALE', 'FEMALE', 'FEMALE', 'MALE', 'FEMALE', 'MALE', 'MALE', 'FEMALE', 'FEMALE', 'MALE',
                       'FEMALE', 'FEMALE', 'MALE', 'FEMALE', 'FEMALE', 'MALE', 'FEMALE', 'FEMALE', 'MALE', 'FEMALE',
                       'MALE', 'MALE', 'MALE', 'FEMALE', 'MALE', 'FEMALE', 'MALE', 'MALE', 'FEMALE', 'MALE', 'MALE',
                       'MALE', 'FEMALE', 'MALE', 'MALE', 'MALE', 'MALE', 'MALE', 'FEMALE', 'MALE', 'MALE', 'MALE',
                       'FEMALE', 'FEMALE', 'FEMALE', 'FEMALE', 'FEMALE', 'MALE', 'MALE', 'FEMALE', 'MALE', 'MALE',
                       'FEMALE', 'MALE', 'FEMALE', 'MALE', 'FEMALE', 'FEMALE', 'MALE', 'FEMALE', 'FEMALE', 'FEMALE',
                       'FEMALE', 'FEMALE', 'FEMALE', 'FEMALE', 'FEMALE', 'MALE', 'FEMALE', 'MALE', 'FEMALE', 'FEMALE',
                       'FEMALE', 'FEMALE', 'MALE', 'FEMALE', 'FEMALE', 'MALE', 'MALE', 'FEMALE', 'FEMALE', 'MALE']

patient_state_names_list = ['Alabama', 'Alabama', 'Alabama', 'Alabama', 'Alabama', 'Alabama', 'Alabama', 'Alabama',
                            'Alabama', 'Alabama', 'Alabama', 'Alabama', 'Alabama', 'Alabama', 'Alabama', 'Alabama',
                            'Alabama', 'Alabama', 'Alabama', 'Alabama', 'Alaska', 'Alaska', 'Alaska', 'Alaska',
                            'Alaska', 'Alaska', 'Alaska', 'Alaska', 'Alaska', 'Alaska', 'Alaska', 'Alaska', 'Alaska',
                            'Alaska', 'Alaska', 'Alaska', 'Alaska', 'Alaska', 'Alaska', 'Alaska', 'Arizona', 'Arizona',
                            'Arizona', 'Arizona', 'Arizona', 'Arizona', 'Arizona', 'Arizona', 'Arizona', 'Arizona',
                            'Arizona', 'Arizona', 'Arizona', 'Arizona', 'Arizona', 'Arizona', 'Arizona', 'Arizona',
                            'Arizona', 'Arizona', 'California', 'California', 'California', 'California', 'California',
                            'California', 'California', 'California', 'California', 'California', 'California',
                            'California', 'California', 'California', 'California', 'California', 'California',
                            'California', 'California', 'California', 'Colorado', 'Colorado', 'Colorado', 'Colorado',
                            'Colorado', 'Colorado', 'Colorado', 'Colorado', 'Colorado', 'Colorado', 'Colorado',
                            'Colorado', 'Colorado', 'Colorado', 'Colorado', 'Colorado', 'Colorado', 'Colorado',
                            'Colorado', 'Colorado', 'Florida', 'Florida', 'Florida', 'Florida', 'Florida', 'Florida',
                            'Florida', 'Florida', 'Florida', 'Florida', 'Florida', 'Florida', 'Florida', 'Florida',
                            'Florida', 'Florida', 'Florida', 'Florida', 'Florida', 'Florida', 'Georgia', 'Georgia',
                            'Georgia', 'Georgia', 'Georgia', 'Georgia', 'Georgia', 'Georgia', 'Georgia', 'Georgia',
                            'Georgia', 'Georgia', 'Georgia', 'Georgia', 'Georgia', 'Georgia', 'Georgia', 'Georgia',
                            'Georgia', 'Georgia', 'Michigan', 'Michigan', 'Michigan', 'Michigan', 'Michigan',
                            'Michigan', 'Michigan', 'Michigan', 'Michigan', 'Michigan', 'Michigan', 'Michigan',
                            'Michigan', 'Michigan', 'Michigan', 'Michigan', 'Michigan', 'Michigan', 'Michigan',
                            'Michigan', 'New York', 'New York', 'New York', 'New York', 'New York', 'New York',
                            'New York', 'New York', 'New York', 'New York', 'New York', 'New York', 'New York',
                            'New York', 'New York', 'New York', 'New York', 'New York', 'New York', 'New York', 'Texas',
                            'Texas', 'Texas', 'Texas', 'Texas', 'Texas', 'Texas', 'Texas', 'Texas', 'Texas', 'Texas',
                            'Texas', 'Texas', 'Texas', 'Texas', 'Texas', 'Texas', 'Texas', 'Texas', 'Texas']

patient_state_codes_list = ['AL', 'AL', 'AL', 'AL', 'AL', 'AL', 'AL', 'AL', 'AL', 'AL', 'AL', 'AL', 'AL', 'AL', 'AL',
                            'AL', 'AL', 'AL', 'AL', 'AL', 'AK', 'AK', 'AK', 'AK', 'AK', 'AK', 'AK', 'AK', 'AK', 'AK',
                            'AK', 'AK', 'AK', 'AK', 'AK', 'AK', 'AK', 'AK', 'AK', 'AK', 'AZ', 'AZ', 'AZ', 'AZ', 'AZ',
                            'AZ', 'AZ', 'AZ', 'AZ', 'AZ', 'AZ', 'AZ', 'AZ', 'AZ', 'AZ', 'AZ', 'AZ', 'AZ', 'AZ', 'AZ',
                            'CA', 'CA', 'CA', 'CA', 'CA', 'CA', 'CA', 'CA', 'CA', 'CA', 'CA', 'CA', 'CA', 'CA', 'CA',
                            'CA', 'CA', 'CA', 'CA', 'CA', 'CO', 'CO', 'CO', 'CO', 'CO', 'CO', 'CO', 'CO', 'CO', 'CO',
                            'CO', 'CO', 'CO', 'CO', 'CO', 'CO', 'CO', 'CO', 'CO', 'CO', 'FL', 'FL', 'FL', 'FL', 'FL',
                            'FL', 'FL', 'FL', 'FL', 'FL', 'FL', 'FL', 'FL', 'FL', 'FL', 'FL', 'FL', 'FL', 'FL', 'FL',
                            'GA', 'GA', 'GA', 'GA', 'GA', 'GA', 'GA', 'GA', 'GA', 'GA', 'GA', 'GA', 'GA', 'GA', 'GA',
                            'GA', 'GA', 'GA', 'GA', 'GA', 'MI', 'MI', 'MI', 'MI', 'MI', 'MI', 'MI', 'MI', 'MI', 'MI',
                            'MI', 'MI', 'MI', 'MI', 'MI', 'MI', 'MI', 'MI', 'MI', 'MI', 'NY', 'NY', 'NY', 'NY', 'NY',
                            'NY', 'NY', 'NY', 'NY', 'NY', 'NY', 'NY', 'NY', 'NY', 'NY', 'NY', 'NY', 'NY', 'NY', 'NY',
                            'TX', 'TX', 'TX', 'TX', 'TX', 'TX', 'TX', 'TX', 'TX', 'TX', 'TX', 'TX', 'TX', 'TX', 'TX',
                            'TX', 'TX', 'TX', 'TX', 'TX']

patient_city_names_list = ['Jefferson', 'Jefferson', 'Jefferson', 'Jefferson', 'Jefferson', 'Jefferson', 'Jefferson',
                           'Mobile', 'Mobile', 'Mobile', 'Mobile', 'Mobile', 'Mobile', 'Mobile', 'Montgomery',
                           'Montgomery', 'Montgomery', 'Montgomery', 'Montgomery', 'Montgomery', 'Adak', 'Adak', 'Adak',
                           'Adak', 'Adak', 'Adak', 'Adak', 'Akhiok', 'Akhiok', 'Akhiok', 'Akhiok', 'Akhiok', 'Akhiok',
                           'Akhiok', 'Akiak', 'Akiak', 'Akiak', 'Akiak', 'Akiak', 'Akiak', 'Avondale', 'Avondale',
                           'Avondale', 'Avondale', 'Avondale', 'Avondale', 'Avondale', 'Benson', 'Benson', 'Benson',
                           'Benson', 'Benson', 'Benson', 'Benson', 'Bisbee', 'Bisbee', 'Bisbee', 'Bisbee', 'Bisbee',
                           'Bisbee', 'Adelanto', 'Adelanto', 'Adelanto', 'Adelanto', 'Adelanto', 'Adelanto', 'Adelanto',
                           'Palo Alto', 'Palo Alto', 'Palo Alto', 'Palo Alto', 'Palo Alto', 'Palo Alto', 'Palo Alto',
                           'Orland', 'Orland', 'Orland', 'Orland', 'Orland', 'Orland', 'Alma', 'Alma', 'Alma', 'Alma',
                           'Alma', 'Alma', 'Alma', 'Brookside', 'Brookside', 'Brookside', 'Brookside', 'Brookside',
                           'Brookside', 'Brookside', 'Denver', 'Denver', 'Denver', 'Denver', 'Denver', 'Denver',
                           'Miami', 'Miami', 'Miami', 'Miami', 'Miami', 'Miami', 'Miami', 'Destin', 'Destin', 'Destin',
                           'Destin', 'Destin', 'Destin', 'Destin', 'Tampa', 'Tampa', 'Tampa', 'Tampa', 'Tampa', 'Tampa',
                           'Atlanta', 'Atlanta', 'Atlanta', 'Atlanta', 'Atlanta', 'Atlanta', 'Atlanta', 'Columbus',
                           'Columbus', 'Columbus', 'Columbus', 'Columbus', 'Columbus', 'Columbus', 'Augusta', 'Augusta',
                           'Augusta', 'Augusta', 'Augusta', 'Augusta', 'Detroit', 'Detroit', 'Detroit', 'Detroit',
                           'Detroit', 'Detroit', 'Detroit', 'Lansing', 'Lansing', 'Lansing', 'Lansing', 'Lansing',
                           'Lansing', 'Lansing', 'Flint', 'Flint', 'Flint', 'Flint', 'Flint', 'Flint', 'New York',
                           'New York', 'New York', 'New York', 'New York', 'New York', 'New York', 'Kingston',
                           'Kingston', 'Kingston', 'Kingston', 'Kingston', 'Kingston', 'Kingston', 'Middletown',
                           'Middletown', 'Middletown', 'Middletown', 'Middletown', 'Middletown', 'Dallas', 'Dallas',
                           'Dallas', 'Dallas', 'Dallas', 'Dallas', 'Dallas', 'Hosuton', 'Hosuton', 'Hosuton', 'Hosuton',
                           'Hosuton', 'Hosuton', 'Hosuton', 'Palano', 'Palano', 'Palano', 'Palano', 'Palano', 'Palano']

patient_city_codes_list = ['AL1', 'AL1', 'AL1', 'AL1', 'AL1', 'AL1', 'AL1', 'AL2', 'AL2', 'AL2', 'AL2', 'AL2', 'AL2',
                           'AL2', 'AL3', 'AL3', 'AL3', 'AL3', 'AL3', 'AL3', 'AK1', 'AK1', 'AK1', 'AK1', 'AK1', 'AK1',
                           'AK1', 'AK2', 'AK2', 'AK2', 'AK2', 'AK2', 'AK2', 'AK2', 'AK3', 'AK3', 'AK3', 'AK3', 'AK3',
                           'AK3', 'AZ1', 'AZ1', 'AZ1', 'AZ1', 'AZ1', 'AZ1', 'AZ1', 'AZ2', 'AZ2', 'AZ2', 'AZ2', 'AZ2',
                           'AZ2', 'AZ2', 'AZ3', 'AZ3', 'AZ3', 'AZ3', 'AZ3', 'AZ3', 'CA1', 'CA1', 'CA1', 'CA1', 'CA1',
                           'CA1', 'CA1', 'CA2', 'CA2', 'CA2', 'CA2', 'CA2', 'CA2', 'CA2', 'CA3', 'CA3', 'CA3', 'CA3',
                           'CA3', 'CA3', 'CO1', 'CO1', 'CO1', 'CO1', 'CO1', 'CO1', 'CO1', 'CO2', 'CO2', 'CO2', 'CO2',
                           'CO2', 'CO2', 'CO2', 'CO3', 'CO3', 'CO3', 'CO3', 'CO3', 'CO3', 'FL1', 'FL1', 'FL1', 'FL1',
                           'FL1', 'FL1', 'FL1', 'FL2', 'FL2', 'FL2', 'FL2', 'FL2', 'FL2', 'FL2', 'FL3', 'FL3', 'FL3',
                           'FL3', 'FL3', 'FL3', 'GA1', 'GA1', 'GA1', 'GA1', 'GA1', 'GA1', 'GA1', 'GA2', 'GA2', 'GA2',
                           'GA2', 'GA2', 'GA2', 'GA2', 'GA3', 'GA3', 'GA3', 'GA3', 'GA3', 'GA3', 'MI1', 'MI1', 'MI1',
                           'MI1', 'MI1', 'MI1', 'MI1', 'MI2', 'MI2', 'MI2', 'MI2', 'MI2', 'MI2', 'MI2', 'MI3', 'MI3',
                           'MI3', 'MI3', 'MI3', 'MI3', 'NY1', 'NY1', 'NY1', 'NY1', 'NY1', 'NY1', 'NY1', 'NY2', 'NY2',
                           'NY2', 'NY2', 'NY2', 'NY2', 'NY2', 'NY3', 'NY3', 'NY3', 'NY3', 'NY3', 'NY3', 'TX1', 'TX1',
                           'TX1', 'TX1', 'TX1', 'TX1', 'TX1', 'TX2', 'TX2', 'TX2', 'TX2', 'TX2', 'TX2', 'TX2', 'TX3',
                           'TX3', 'TX3', 'TX3', 'TX3', 'TX3']

hospital_names_list = ['Ellery Hospital', 'Kristin Hospital', 'Marcella Hospital', 'Gene Hospital', 'Ashleigh Hospital',
                       'Justice Hospital', 'Santiago Hospital', 'Trayvon Hospital', 'Kalen Hospital', 'Ewald Hospital',
                       'Ferrell Hospital', 'Kelton Hospital', 'Felicia Hospital', 'Corrie Hospital', 'Versa Hospital',
                       'Pranav Hospital', 'Aleen Hospital', 'Tony Hospital', 'Mason Hospital', 'Ford Hospital',
                       'Tyra Hospital', 'Cedric Hospital', 'Deborah Hospital', 'Josiephine Hospital', 'Doll Hospital',
                       'Errol Hospital', 'Ingram Hospital', 'Kaylen Hospital', 'Sheyla Hospital', 'Lahoma Hospital',
                       'Flor Hospital', 'Theta Hospital', 'Jadyn Hospital', 'Zaida Hospital', 'Griffith Hospital',
                       'Selma Hospital', 'Serenity Hospital', 'Davey Hospital', 'Olivia Hospital', 'Tresa Hospital',
                       'Rashida Hospital', 'Arely Hospital', 'Buell Hospital', 'Gilda Hospital', 'Lewis Hospital',
                       'Bjorn Hospital', 'Keifer Hospital', 'Althea Hospital', 'Mansfield Hospital', 'Daisie Hospital',
                       'Thresa Hospital', 'Myrta Hospital', 'Kem Hospital', 'Jaheem Hospital', 'Lucky Hospital',
                       'Allan Hospital', 'Dyan Hospital', 'Jonatan Hospital', 'Tomas Hospital', 'Astrid Hospital',
                       'Campbell Hospital', 'Christeen Hospital', 'Gladys Hospital', 'Margaretha Hospital',
                       'Roel Hospital', 'Early Hospital', 'Zion Hospital', 'Alejandro Hospital', 'Dixie Hospital',
                       'Ferne Hospital', 'Mauro Hospital', 'Darrick Hospital', 'Mariann Hospital', 'Captain Hospital',
                       'Merwin Hospital', 'Sage Hospital', 'Selena Hospital', 'Claudio Hospital', 'Rustin Hospital',
                       'Johnnie Hospital', 'Niles Hospital', 'Kenya Hospital', 'Taylor Hospital', 'Emmitt Hospital',
                       'Erwin Hospital', 'Alexia Hospital', 'Arland Hospital', 'Elbert Hospital', 'Elenora Hospital',
                       'Elie Hospital', 'Margarette Hospital', 'Joye Hospital', 'Ines Hospital', 'Collie Hospital',
                       'Arlington Hospital', 'Liana Hospital', 'Lenny Hospital', 'Tiney Hospital', 'Orene Hospital',
                       'Nellie Hospital']

hospital_state_names_list = ['Alabama', 'Alabama', 'Alabama', 'Alabama', 'Alabama', 'Alabama', 'Alabama', 'Alabama',
                             'Alabama', 'Alabama', 'Alabama', 'Alabama', 'Alabama', 'Alabama', 'Alabama', 'Alabama',
                             'Alabama', 'Alabama', 'Alabama', 'Alabama', 'Alaska', 'Alaska', 'Alaska', 'Alaska',
                             'Alaska', 'Alaska', 'Alaska', 'Alaska', 'Alaska', 'Alaska', 'Alaska', 'Alaska', 'Alaska',
                             'Alaska', 'Alaska', 'Alaska', 'Alaska', 'Alaska', 'Alaska', 'Alaska', 'Arizona', 'Arizona',
                             'Arizona', 'Arizona', 'Arizona', 'Arizona', 'Arizona', 'Arizona', 'Arizona', 'Arizona',
                             'Arizona', 'Arizona', 'Arizona', 'Arizona', 'Arizona', 'Arizona', 'Arizona', 'Arizona',
                             'Arizona', 'Arizona', 'California', 'California', 'California', 'California', 'California',
                             'California', 'California', 'California', 'California', 'California', 'California',
                             'California', 'California', 'California', 'California', 'California', 'California',
                             'California', 'California', 'California', 'Colorado', 'Colorado', 'Colorado', 'Colorado',
                             'Colorado', 'Colorado', 'Colorado', 'Colorado', 'Colorado', 'Colorado', 'Colorado',
                             'Colorado', 'Colorado', 'Colorado', 'Colorado', 'Colorado', 'Colorado', 'Colorado',
                             'Colorado', 'Colorado']

hospital_state_codes_list = ['AL', 'AL', 'AL', 'AL', 'AL', 'AL', 'AL', 'AL', 'AL', 'AL', 'AL', 'AL', 'AL', 'AL', 'AL',
                             'AL', 'AL', 'AL', 'AL', 'AL', 'AK', 'AK', 'AK', 'AK', 'AK', 'AK', 'AK', 'AK', 'AK', 'AK',
                             'AK', 'AK', 'AK', 'AK', 'AK', 'AK', 'AK', 'AK', 'AK', 'AK', 'AZ', 'AZ', 'AZ', 'AZ', 'AZ',
                             'AZ', 'AZ', 'AZ', 'AZ', 'AZ', 'AZ', 'AZ', 'AZ', 'AZ', 'AZ', 'AZ', 'AZ', 'AZ', 'AZ', 'AZ',
                             'CA', 'CA', 'CA', 'CA', 'CA', 'CA', 'CA', 'CA', 'CA', 'CA', 'CA', 'CA', 'CA', 'CA', 'CA',
                             'CA', 'CA', 'CA', 'CA', 'CA', 'CO', 'CO', 'CO', 'CO', 'CO', 'CO', 'CO', 'CO', 'CO', 'CO',
                             'CO', 'CO', 'CO', 'CO', 'CO', 'CO', 'CO', 'CO', 'CO', 'CO']

hospital_city_names_list = ['Jefferson', 'Jefferson', 'Jefferson', 'Jefferson', 'Jefferson', 'Jefferson', 'Jefferson',
                            'Jefferson', 'Jefferson', 'Jefferson', 'Mobile', 'Mobile', 'Mobile', 'Mobile', 'Mobile',
                            'Mobile', 'Mobile', 'Mobile', 'Mobile', 'Mobile', 'Adak', 'Adak', 'Adak', 'Adak', 'Adak',
                            'Adak', 'Adak', 'Adak', 'Adak', 'Adak', 'Akhiok', 'Akhiok', 'Akhiok', 'Akhiok', 'Akhiok',
                            'Akhiok', 'Akhiok', 'Akhiok', 'Akhiok', 'Akhiok', 'Avondale', 'Avondale', 'Avondale',
                            'Avondale', 'Avondale', 'Avondale', 'Avondale', 'Avondale', 'Avondale', 'Avondale',
                            'Benson', 'Benson', 'Benson', 'Benson', 'Benson', 'Benson', 'Benson', 'Benson', 'Benson',
                            'Benson', 'Adelanto', 'Adelanto', 'Adelanto', 'Adelanto', 'Adelanto', 'Adelanto',
                            'Adelanto', 'Adelanto', 'Adelanto', 'Adelanto', 'Palo Alto', 'Palo Alto', 'Palo Alto',
                            'Palo Alto', 'Palo Alto', 'Palo Alto', 'Palo Alto', 'Palo Alto', 'Palo Alto', 'Palo Alto',
                            'Alma', 'Alma', 'Alma', 'Alma', 'Alma', 'Alma', 'Alma', 'Alma', 'Alma', 'Alma', 'Brookside',
                            'Brookside', 'Brookside', 'Brookside', 'Brookside', 'Brookside', 'Brookside', 'Brookside',
                            'Brookside', 'Brookside']

hospital_city_codes_list = ['AL1', 'AL1', 'AL1', 'AL1', 'AL1', 'AL1', 'AL1', 'AL1', 'AL1', 'AL1', 'AL2', 'AL2', 'AL2',
                            'AL2', 'AL2', 'AL2', 'AL2', 'AL2', 'AL2', 'AL2', 'AK1', 'AK1', 'AK1', 'AK1', 'AK1', 'AK1',
                            'AK1', 'AK1', 'AK1', 'AK1', 'AK2', 'AK2', 'AK2', 'AK2', 'AK2', 'AK2', 'AK2', 'AK2', 'AK2',
                            'AK2', 'AZ1', 'AZ1', 'AZ1', 'AZ1', 'AZ1', 'AZ1', 'AZ1', 'AZ1', 'AZ1', 'AZ1', 'AZ2', 'AZ2',
                            'AZ2', 'AZ2', 'AZ2', 'AZ2', 'AZ2', 'AZ2', 'AZ2', 'AZ2', 'CA1', 'CA1', 'CA1', 'CA1', 'CA1',
                            'CA1', 'CA1', 'CA1', 'CA1', 'CA1', 'CA2', 'CA2', 'CA2', 'CA2', 'CA2', 'CA2', 'CA2', 'CA2',
                            'CA2', 'CA2', 'CO1', 'CO1', 'CO1', 'CO1', 'CO1', 'CO1', 'CO1', 'CO1', 'CO1', 'CO1', 'CO2',
                            'CO2', 'CO2', 'CO2', 'CO2', 'CO2', 'CO2', 'CO2', 'CO2', 'CO2']

hospital_id_list = ['HOSP1', 'HOSP2', 'HOSP3', 'HOSP4', 'HOSP5', 'HOSP6', 'HOSP7', 'HOSP8', 'HOSP9', 'HOSP10', 'HOSP11',
                    'HOSP12', 'HOSP13', 'HOSP14', 'HOSP15', 'HOSP16', 'HOSP17', 'HOSP18', 'HOSP19', 'HOSP20', 'HOSP21',
                    'HOSP22', 'HOSP23', 'HOSP24', 'HOSP25', 'HOSP26', 'HOSP27', 'HOSP28', 'HOSP29', 'HOSP30', 'HOSP31',
                    'HOSP32', 'HOSP33', 'HOSP34', 'HOSP35', 'HOSP36', 'HOSP37', 'HOSP38', 'HOSP39', 'HOSP40', 'HOSP41',
                    'HOSP42', 'HOSP43', 'HOSP44', 'HOSP45', 'HOSP46', 'HOSP47', 'HOSP48', 'HOSP49', 'HOSP50', 'HOSP51',
                    'HOSP52', 'HOSP53', 'HOSP54', 'HOSP55', 'HOSP56', 'HOSP57', 'HOSP58', 'HOSP59', 'HOSP60', 'HOSP61',
                    'HOSP62', 'HOSP63', 'HOSP64', 'HOSP65', 'HOSP66', 'HOSP67', 'HOSP68', 'HOSP69', 'HOSP70', 'HOSP71',
                    'HOSP72', 'HOSP73', 'HOSP74', 'HOSP75', 'HOSP76', 'HOSP77', 'HOSP78', 'HOSP79', 'HOSP80', 'HOSP81',
                    'HOSP82', 'HOSP83', 'HOSP84', 'HOSP85', 'HOSP86', 'HOSP87', 'HOSP88', 'HOSP89', 'HOSP90', 'HOSP91',
                    'HOSP92', 'HOSP93', 'HOSP94', 'HOSP95', 'HOSP96', 'HOSP97', 'HOSP98', 'HOSP99', 'HOSP100']

doctor_id_list = ['HOSP1_1', 'HOSP1_2', 'HOSP1_3', 'HOSP1_4', 'HOSP2_1', 'HOSP2_2', 'HOSP2_3', 'HOSP2_4', 'HOSP3_1',
                  'HOSP3_2', 'HOSP3_3', 'HOSP3_4', 'HOSP4_1', 'HOSP4_2', 'HOSP4_3', 'HOSP4_4', 'HOSP5_1', 'HOSP5_2',
                  'HOSP5_3', 'HOSP5_4', 'HOSP6_1', 'HOSP6_2', 'HOSP6_3', 'HOSP6_4', 'HOSP7_1', 'HOSP7_2', 'HOSP7_3',
                  'HOSP7_4', 'HOSP8_1', 'HOSP8_2', 'HOSP8_3', 'HOSP8_4', 'HOSP9_1', 'HOSP9_2', 'HOSP9_3', 'HOSP9_4',
                  'HOSP10_1', 'HOSP10_2', 'HOSP10_3', 'HOSP10_4', 'HOSP11_1', 'HOSP11_2', 'HOSP11_3', 'HOSP11_4',
                  'HOSP12_1', 'HOSP12_2', 'HOSP12_3', 'HOSP12_4', 'HOSP13_1', 'HOSP13_2', 'HOSP13_3', 'HOSP13_4',
                  'HOSP14_1', 'HOSP14_2', 'HOSP14_3', 'HOSP14_4', 'HOSP15_1', 'HOSP15_2', 'HOSP15_3', 'HOSP15_4',
                  'HOSP16_1', 'HOSP16_2', 'HOSP16_3', 'HOSP16_4', 'HOSP17_1', 'HOSP17_2', 'HOSP17_3', 'HOSP17_4',
                  'HOSP18_1', 'HOSP18_2', 'HOSP18_3', 'HOSP18_4', 'HOSP19_1', 'HOSP19_2', 'HOSP19_3', 'HOSP19_4',
                  'HOSP20_1', 'HOSP20_2', 'HOSP20_3', 'HOSP20_4', 'HOSP21_1', 'HOSP21_2', 'HOSP21_3', 'HOSP21_4',
                  'HOSP22_1', 'HOSP22_2', 'HOSP22_3', 'HOSP22_4', 'HOSP23_1', 'HOSP23_2', 'HOSP23_3', 'HOSP23_4',
                  'HOSP24_1', 'HOSP24_2', 'HOSP24_3', 'HOSP24_4', 'HOSP25_1', 'HOSP25_2', 'HOSP25_3', 'HOSP25_4',
                  'HOSP26_1', 'HOSP26_2', 'HOSP26_3', 'HOSP26_4', 'HOSP27_1', 'HOSP27_2', 'HOSP27_3', 'HOSP27_4',
                  'HOSP28_1', 'HOSP28_2', 'HOSP28_3', 'HOSP28_4', 'HOSP29_1', 'HOSP29_2', 'HOSP29_3', 'HOSP29_4',
                  'HOSP30_1', 'HOSP30_2', 'HOSP30_3', 'HOSP30_4', 'HOSP31_1', 'HOSP31_2', 'HOSP31_3', 'HOSP31_4',
                  'HOSP32_1', 'HOSP32_2', 'HOSP32_3', 'HOSP32_4', 'HOSP33_1', 'HOSP33_2', 'HOSP33_3', 'HOSP33_4',
                  'HOSP34_1', 'HOSP34_2', 'HOSP34_3', 'HOSP34_4', 'HOSP35_1', 'HOSP35_2', 'HOSP35_3', 'HOSP35_4',
                  'HOSP36_1', 'HOSP36_2', 'HOSP36_3', 'HOSP36_4', 'HOSP37_1', 'HOSP37_2', 'HOSP37_3', 'HOSP37_4',
                  'HOSP38_1', 'HOSP38_2', 'HOSP38_3', 'HOSP38_4', 'HOSP39_1', 'HOSP39_2', 'HOSP39_3', 'HOSP39_4',
                  'HOSP40_1', 'HOSP40_2', 'HOSP40_3', 'HOSP40_4', 'HOSP41_1', 'HOSP41_2', 'HOSP41_3', 'HOSP41_4',
                  'HOSP42_1', 'HOSP42_2', 'HOSP42_3', 'HOSP42_4', 'HOSP43_1', 'HOSP43_2', 'HOSP43_3', 'HOSP43_4',
                  'HOSP44_1', 'HOSP44_2', 'HOSP44_3', 'HOSP44_4', 'HOSP45_1', 'HOSP45_2', 'HOSP45_3', 'HOSP45_4',
                  'HOSP46_1', 'HOSP46_2', 'HOSP46_3', 'HOSP46_4', 'HOSP47_1', 'HOSP47_2', 'HOSP47_3', 'HOSP47_4',
                  'HOSP48_1', 'HOSP48_2', 'HOSP48_3', 'HOSP48_4', 'HOSP49_1', 'HOSP49_2', 'HOSP49_3', 'HOSP49_4',
                  'HOSP50_1', 'HOSP50_2', 'HOSP50_3', 'HOSP50_4', 'HOSP51_1', 'HOSP51_2', 'HOSP51_3', 'HOSP51_4',
                  'HOSP52_1', 'HOSP52_2', 'HOSP52_3', 'HOSP52_4', 'HOSP53_1', 'HOSP53_2', 'HOSP53_3', 'HOSP53_4',
                  'HOSP54_1', 'HOSP54_2', 'HOSP54_3', 'HOSP54_4', 'HOSP55_1', 'HOSP55_2', 'HOSP55_3', 'HOSP55_4',
                  'HOSP56_1', 'HOSP56_2', 'HOSP56_3', 'HOSP56_4', 'HOSP57_1', 'HOSP57_2', 'HOSP57_3', 'HOSP57_4',
                  'HOSP58_1', 'HOSP58_2', 'HOSP58_3', 'HOSP58_4', 'HOSP59_1', 'HOSP59_2', 'HOSP59_3', 'HOSP59_4',
                  'HOSP60_1', 'HOSP60_2', 'HOSP60_3', 'HOSP60_4', 'HOSP61_1', 'HOSP61_2', 'HOSP61_3', 'HOSP61_4',
                  'HOSP62_1', 'HOSP62_2', 'HOSP62_3', 'HOSP62_4', 'HOSP63_1', 'HOSP63_2', 'HOSP63_3', 'HOSP63_4',
                  'HOSP64_1', 'HOSP64_2', 'HOSP64_3', 'HOSP64_4', 'HOSP65_1', 'HOSP65_2', 'HOSP65_3', 'HOSP65_4',
                  'HOSP66_1', 'HOSP66_2', 'HOSP66_3', 'HOSP66_4', 'HOSP67_1', 'HOSP67_2', 'HOSP67_3', 'HOSP67_4',
                  'HOSP68_1', 'HOSP68_2', 'HOSP68_3', 'HOSP68_4', 'HOSP69_1', 'HOSP69_2', 'HOSP69_3', 'HOSP69_4',
                  'HOSP70_1', 'HOSP70_2', 'HOSP70_3', 'HOSP70_4', 'HOSP71_1', 'HOSP71_2', 'HOSP71_3', 'HOSP71_4',
                  'HOSP72_1', 'HOSP72_2', 'HOSP72_3', 'HOSP72_4', 'HOSP73_1', 'HOSP73_2', 'HOSP73_3', 'HOSP73_4',
                  'HOSP74_1', 'HOSP74_2', 'HOSP74_3', 'HOSP74_4', 'HOSP75_1', 'HOSP75_2', 'HOSP75_3', 'HOSP75_4',
                  'HOSP76_1', 'HOSP76_2', 'HOSP76_3', 'HOSP76_4', 'HOSP77_1', 'HOSP77_2', 'HOSP77_3', 'HOSP77_4',
                  'HOSP78_1', 'HOSP78_2', 'HOSP78_3', 'HOSP78_4', 'HOSP79_1', 'HOSP79_2', 'HOSP79_3', 'HOSP79_4',
                  'HOSP80_1', 'HOSP80_2', 'HOSP80_3', 'HOSP80_4', 'HOSP81_1', 'HOSP81_2', 'HOSP81_3', 'HOSP81_4',
                  'HOSP82_1', 'HOSP82_2', 'HOSP82_3', 'HOSP82_4', 'HOSP83_1', 'HOSP83_2', 'HOSP83_3', 'HOSP83_4',
                  'HOSP84_1', 'HOSP84_2', 'HOSP84_3', 'HOSP84_4', 'HOSP85_1', 'HOSP85_2', 'HOSP85_3', 'HOSP85_4',
                  'HOSP86_1', 'HOSP86_2', 'HOSP86_3', 'HOSP86_4', 'HOSP87_1', 'HOSP87_2', 'HOSP87_3', 'HOSP87_4',
                  'HOSP88_1', 'HOSP88_2', 'HOSP88_3', 'HOSP88_4', 'HOSP89_1', 'HOSP89_2', 'HOSP89_3', 'HOSP89_4',
                  'HOSP90_1', 'HOSP90_2', 'HOSP90_3', 'HOSP90_4', 'HOSP91_1', 'HOSP91_2', 'HOSP91_3', 'HOSP91_4',
                  'HOSP92_1', 'HOSP92_2', 'HOSP92_3', 'HOSP92_4', 'HOSP93_1', 'HOSP93_2', 'HOSP93_3', 'HOSP93_4',
                  'HOSP94_1', 'HOSP94_2', 'HOSP94_3', 'HOSP94_4', 'HOSP95_1', 'HOSP95_2', 'HOSP95_3', 'HOSP95_4',
                  'HOSP96_1', 'HOSP96_2', 'HOSP96_3', 'HOSP96_4', 'HOSP97_1', 'HOSP97_2', 'HOSP97_3', 'HOSP97_4',
                  'HOSP98_1', 'HOSP98_2', 'HOSP98_3', 'HOSP98_4', 'HOSP99_1', 'HOSP99_2', 'HOSP99_3', 'HOSP99_4',
                  'HOSP100_1', 'HOSP100_2', 'HOSP100_3', 'HOSP100_4']

diagnosis_id_list = ['DIA1', 'DIA2', 'DIA3', 'DIA4', 'DIA5', 'DIA6', 'DIA7', 'DIA8', 'DIA9', 'DIA10', 'DIA11', 'DIA12',
                     'DIA13', 'DIA14', 'DIA15', 'DIA16', 'DIA17', 'DIA18', 'DIA19', 'DIA20', 'DIA21', 'DIA22', 'DIA23',
                     'DIA24', 'DIA25', 'DIA26', 'DIA27', 'DIA28', 'DIA29', 'DIA30', 'DIA31', 'DIA32', 'DIA33', 'DIA34',
                     'DIA35', 'DIA36', 'DIA37', 'DIA38', 'DIA39', 'DIA40', 'DIA41', 'DIA42', 'DIA43', 'DIA44', 'DIA45',
                     'DIA46', 'DIA47', 'DIA48', 'DIA49', 'DIA50', 'DIA51', 'DIA52', 'DIA53', 'DIA54', 'DIA55', 'DIA56',
                     'DIA57', 'DIA58', 'DIA59', 'DIA60', 'DIA61', 'DIA62', 'DIA63', 'DIA64', 'DIA65', 'DIA66', 'DIA67',
                     'DIA68', 'DIA69', 'DIA70', 'DIA71', 'DIA72', 'DIA73', 'DIA74', 'DIA75', 'DIA76', 'DIA77', 'DIA78',
                     'DIA79', 'DIA80', 'DIA81', 'DIA82', 'DIA83', 'DIA84', 'DIA85', 'DIA86', 'DIA87', 'DIA88', 'DIA89',
                     'DIA90', 'DIA91', 'DIA92', 'DIA93', 'DIA94', 'DIA95', 'DIA96', 'DIA97', 'DIA98', 'DIA99', 'DIA100']

procedure_type_list = ['SURGERY', 'SURGERY', 'SURGERY', 'SURGERY', 'SURGERY', 'SURGERY', 'SURGERY', 'SURGERY',
                       'SURGERY', 'SURGERY', 'SURGERY', 'SURGERY', 'SURGERY', 'SURGERY', 'SURGERY', 'SURGERY',
                       'SURGERY', 'SURGERY', 'SURGERY', 'SURGERY', 'SURGERY', 'SURGERY', 'SURGERY', 'SURGERY',
                       'SURGERY', 'SURGERY', 'SURGERY', 'SURGERY', 'SURGERY', 'SURGERY', 'SURGERY', 'SURGERY',
                       'SURGERY', 'SURGERY', 'SURGERY', 'SURGERY', 'SURGERY', 'SURGERY', 'SURGERY', 'SURGERY',
                       'MEDICAL', 'MEDICAL', 'MEDICAL', 'MEDICAL', 'MEDICAL', 'MEDICAL', 'MEDICAL', 'MEDICAL',
                       'MEDICAL', 'MEDICAL', 'MEDICAL', 'MEDICAL', 'MEDICAL', 'MEDICAL', 'MEDICAL', 'MEDICAL',
                       'MEDICAL', 'MEDICAL', 'MEDICAL', 'MEDICAL', 'MEDICAL', 'MEDICAL', 'MEDICAL', 'MEDICAL',
                       'MEDICAL', 'MEDICAL', 'MEDICAL', 'MEDICAL', 'MEDICAL', 'MEDICAL', 'MEDICAL', 'MEDICAL',
                       'MEDICAL', 'MEDICAL', 'MEDICAL', 'MEDICAL', 'MEDICAL', 'MEDICAL', 'MEDICAL', 'MEDICAL',
                       'MEDICAL', 'MEDICAL', 'MEDICAL', 'MEDICAL', 'MEDICAL', 'MEDICAL', 'MEDICAL', 'MEDICAL',
                       'MEDICAL', 'MEDICAL', 'MEDICAL', 'MEDICAL', 'MEDICAL', 'MEDICAL', 'MEDICAL', 'MEDICAL',
                       'MEDICAL', 'MEDICAL', 'MEDICAL', 'MEDICAL']

procedure_name_list = ['Heart transplant or implant of heart assist system w MCC',
                       'Trach w MV 96+ hrs or PDX exc face, mouth & neck w/o maj O.R.',
                       'Liver transplant w MCC or intestinal transplant', 'Lung transplant',
                       'Simultaneous pancreas/kidney transplant', 'Bone marrow transplant', 'Pancreas transplant',
                       'Tracheostomy for face,mouth & neck diagnoses w MCC',
                       'Intracranial vascular procedures w PDX hemorrhage w MCC', 'Major chest procedures w CC',
                       'Periph/cranial nerve & other nerv syst proc w MCC', 'Spinal procedures w MCC',
                       'Ventricular shunt procedures w MCC p', 'Carotid artery stent procedure w MCC',
                       'Extracranial procedures w MCC', 'Orbital procedures w CC/MCC',
                       'Major head & neck procedures w/o CC/MCC', 'Sinus & mastoid procedures w/o CC/MCC',
                       'Cardiac valve & oth maj cardiothoracic proc w card cath w CC',
                       'Cardiac defibrillator implant w/o cardiac cath w/o MCC',
                       'Other cardiothoracic procedures w MCC', 'Coronary bypass w/o cardiac cath w/o MCC',
                       'Major cardiovasc procedures w MCC or thoracic aortic anuerysm repair',
                       'Permanent cardiac pacemaker implant w/o CC/MCC', 'AICD lead & generator procedures',
                       'Vein ligation & stripping', 'Other circulatory system O.R. procedures',
                       'Stomach, esophageal & duodenal proc w/o CC/MCC', 'Major small & large bowel procedures w MCC',
                       'Peritoneal adhesiolysis w/o CC/MCC', 'Appendectomy w complicated principal diag w MCC',
                       'Anal & stomal procedures w/o CC/MCC', 'Inguinal & femoral hernia procedures w MCC',
                       'Pancreas, liver & shunt procedures w/o CC/MCC',
                       'Biliary tract proc except only cholecyst w or w/o c.d.e. w MCC',
                       'Major joint replacement or reattachment of lower extremity w/o MCC',
                       'Cervical spinal fusion w MCC', 'Other skin, subcut tiss & breast proc w/o CC/MCC',
                       'Mastectomy for malignancy w CC/MCC', 'Kidney & ureter procedures for non-neoplasm w CC',
                       'Chronic obstructive pulmonary disease w MCC', 'Full term neonate w major problems',
                       'Neonate w other significant problems', 'Normal newborn',
                       'Infections, female reproductive system w/o CC/MCC',
                       'Menstrual & other female reproductive system disorders w CC/MCC',
                       'Inflammation of the male reproductive system w/o MCC',
                       'Other male reproductive system diagnoses w CC/MCC', 'Kidney & urinary tract infections w/o MCC',
                       'Urinary stones w esw lithotripsy w CC/MCC', 'Urethral stricture',
                       'Other kidney & urinary tract diagnoses w MCC', 'Nutritional & misc metabolic disorders w/o MCC',
                       'Inborn errors of metabolism', 'Endocrine disorders w MCC',
                       'Tendonitis, myositis & bursitis w/o MCC',
                       'Aftercare, musculoskeletal system & connective tissue w MCC',
                       'Connective tissue disorders w/o CC/MCC', 'Septic arthritis w MCC', 'Fractures of femur w/o MCC',
                       'Fractures of hip & pelvis w MCC', 'Disorders of liver except malig,cirr,alc hepa w/o CC/MCC',
                       'Disorders of the biliary tract w MCC', 'Cirrhosis & alcoholic hepatitis w/o CC/MCC',
                       'Malignancy of hepatobiliary system or pancreas w MCC', 'G.I. hemorrhage w/o CC/MCC',
                       'Complicated peptic ulcer w MCC', 'Hypertension w/o MCC',
                       'Cardiac congenital & valvular disorders w MCC', 'Acute & subacute endocarditis w/o CC/MCC',
                       'Heart failure & shock w MCC', 'Bronchitis & asthma w/o CC/MCC', 'Respiratory signs & symptoms',
                       'Spinal disorders & injuries w CC/MCC', 'Nervous system neoplasms w MCC',
                       'Degenerative nervous system disorders w MCC', 'Multiple sclerosis & cerebellar ataxia w MCC',
                       'Acute ischemic stroke w use of thrombolytic agent w MCC',
                       'Intracranial hemorrhage or cerebral infarction w MCC',
                       'Nonspecific cva & precerebral occlusion w/o infarct w MCC', 'Transient ischemia',
                       'Nonspecific cerebrovascular disorders w MCC', 'Cranial & peripheral nerve disorders w MCC',
                       'Viral meningitis w CC/MCC', 'Hypertensive encephalopathy w/o CC/MCC',
                       'Nontraumatic stupor & coma w MCC', 'Traumatic stupor & coma, coma >1 hr w MCC',
                       'Traumatic stupor & coma, coma <1 hr w/o CC/MCC', 'Concussion w MCC',
                       'Other disorders of nervous system w MCC',
                       'Bacterial & tuberculous infections of nervous system w/o CC/MCC',
                       'Non-bacterial infect of nervous sys exc viral meningitis w MCC', 'Seizures w MCC',
                       'Headaches w MCC', 'Ear, nose, mouth & throat malignancy w', 'Acute major eye infections',
                       'Dental & Oral Diseases ', 'Major chest trauma', 'Interstitial lung disease ',
                       'Nasal trauma & deformity w MCC']

claim_type_list = ['INPATIENT', 'INPATIENT', 'INPATIENT', 'INPATIENT', 'INPATIENT', 'INPATIENT', 'INPATIENT',
                   'INPATIENT', 'INPATIENT', 'INPATIENT', 'INPATIENT', 'INPATIENT', 'INPATIENT', 'INPATIENT',
                   'INPATIENT', 'INPATIENT', 'INPATIENT', 'INPATIENT', 'INPATIENT', 'INPATIENT', 'INPATIENT',
                   'INPATIENT', 'INPATIENT', 'INPATIENT', 'INPATIENT', 'INPATIENT', 'INPATIENT', 'INPATIENT',
                   'INPATIENT', 'INPATIENT', 'INPATIENT', 'INPATIENT', 'INPATIENT', 'INPATIENT', 'INPATIENT',
                   'INPATIENT', 'INPATIENT', 'INPATIENT', 'INPATIENT', 'INPATIENT', 'OUTPATIENT', 'OUTPATIENT',
                   'OUTPATIENT', 'OUTPATIENT', 'OUTPATIENT', 'OUTPATIENT', 'OUTPATIENT', 'OUTPATIENT', 'OUTPATIENT',
                   'OUTPATIENT', 'OUTPATIENT', 'OUTPATIENT', 'OUTPATIENT', 'OUTPATIENT', 'OUTPATIENT', 'OUTPATIENT',
                   'OUTPATIENT', 'OUTPATIENT', 'OUTPATIENT', 'OUTPATIENT', 'OUTPATIENT', 'OUTPATIENT', 'OUTPATIENT',
                   'OUTPATIENT', 'OUTPATIENT', 'OUTPATIENT', 'OUTPATIENT', 'OUTPATIENT', 'OUTPATIENT', 'OUTPATIENT',
                   'OUTPATIENT', 'OUTPATIENT', 'OUTPATIENT', 'OUTPATIENT', 'OUTPATIENT', 'OUTPATIENT', 'OUTPATIENT',
                   'OUTPATIENT', 'OUTPATIENT', 'OUTPATIENT', 'OUTPATIENT', 'OUTPATIENT', 'OUTPATIENT', 'OUTPATIENT',
                   'OUTPATIENT', 'OUTPATIENT', 'OUTPATIENT', 'OUTPATIENT', 'OUTPATIENT', 'OUTPATIENT', 'OUTPATIENT',
                   'OUTPATIENT', 'OUTPATIENT', 'OUTPATIENT', 'OUTPATIENT', 'OUTPATIENT', 'OUTPATIENT', 'OUTPATIENT',
                   'OUTPATIENT', 'OUTPATIENT']


def get_claim_data():
    patient_id_number = random.randint(0, 199)
    patient_id = patient_id_list[patient_id_number]
    patient_name = patient_names_list[patient_id_number]
    patient_age = patient_age_list[patient_id_number]
    patient_gender = patient_gender_list[patient_id_number]
    patient_state_code = patient_state_codes_list[patient_id_number]
    patient_state_name = patient_state_names_list[patient_id_number]
    patient_city_name = patient_city_names_list[patient_id_number]
    patient_city_code = patient_city_codes_list[patient_id_number]

    hospital_id_number = random.randint(0, 99)
    hospital_id = hospital_id_list[hospital_id_number]
    hospital_name = hospital_names_list[hospital_id_number]
    hospital_state_name = hospital_state_names_list[hospital_id_number]
    hospital_state_code = hospital_state_codes_list[hospital_id_number]
    hospital_city_name = hospital_city_names_list[hospital_id_number]
    hospital_city_code = hospital_city_codes_list[hospital_id_number]

    doctor_id_number = random.randint(0, 3)
    doctor_id = doctor_id_list[hospital_id_number * 4 + doctor_id_number]

    claim_number = random.randint(0, 99)
    diagnosis_id = diagnosis_id_list[claim_number]
    procedure_name = procedure_name_list[claim_number]
    procedure_type = procedure_type_list[claim_number]
    claim_amount = 0.0
    if procedure_type == "SURGERY":
        claim_amount = float(random.randint(300, 5000) * 10)
    elif procedure_type == "MEDICAL":
        claim_amount = float(random.randint(100, 2000) * 10)
    claim_type = claim_type_list[claim_number]

    timestamp = datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3]
    timestamp = timestamp + "Z"
    date = datetime.utcnow().strftime("%d")
    month = datetime.utcnow().strftime("%m")
    year = datetime.utcnow().strftime("%Y")
    hour = datetime.utcnow().strftime("%H")

    claim_id = str(int(time.time() * 10000000))

    my_dict = {
        "datetime": timestamp,
        "claim_id": claim_id,
        "hour": hour,
        "date": date,
        "month": month,
        "year": year,
        "patient_id": patient_id,
        "patient_name": patient_name,
        "patient_age": patient_age,
        "patient_gender": patient_gender,
        "patient_state_code": patient_state_code,
        "patient_state_name": patient_state_name,
        "patient_city_name": patient_city_name,
        "patient_city_code": patient_city_code,
        "hospital_id": hospital_id,
        "hospital_name": hospital_name,
        "hospital_state_name": hospital_state_name,
        "hospital_state_code": hospital_state_code,
        "hospital_city_name": hospital_city_name,
        "hospital_city_code": hospital_city_code,
        "claim_type": claim_type,
        "claim_amount": claim_amount,
        "diagnosis_id": diagnosis_id,
        "procedure_name": procedure_name,
        "procedure_type": procedure_type,
        "doctor_id": doctor_id
    }
    return my_dict


def dict_to_binary(the_dict):
    d = json.dumps(the_dict)
    binary = str.encode(d)
    return binary


def main():
    producer = KafkaProducer(bootstrap_servers=[kafka_host + ':9092'])

    i = 0
    while True:
        i += 1
        time.sleep(1)
        my_dict = get_claim_data()
        value = dict_to_binary(my_dict)
        producer.send(topic=topic, value=value)
        if i % 100 == 0:
            print(i)


if __name__ == "__main__":
    main()
