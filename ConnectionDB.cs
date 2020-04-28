using System;
using System.Configuration;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Music.Contract;
using Npgsql;
//Comentário feito por Walter
namespace Music.Service.DB
//ALTERAÇÃO FEITA POR WALDINEY TÍTULO
{
    public class ConnectionDB : IDisposable
    {
        //Comentário Waldiney
        private NpgsqlConnection conn = null;
        private string _server = Properties.parametros.Default.HostBanco;
        private string _port = Properties.parametros.Default.PortaBanco;
        private string _user = Properties.parametros.Default.UserBanco;
        private string _password = Properties.parametros.Default.SenhaBanco;
        private string _database = Properties.parametros.Default.NomeBanco;

        public ConnectionDB()
        {
            
            conn = new NpgsqlConnection("Server="+_server+";Port="+_port+ ";User Id="+_user+ ";Password="+_password+ ";Database="+_database+";");
            conn.Open();
        }
        public void Dispose()
        {
            conn.Close();
        }
        internal bool NewUser(Usuario user)
        {
            bool ok = true;
            var instrucao = string.Format("insert into usuario (nome, cpf, email, senha, nivel, status) values(@NOME, @CPF, @EMAIL, @SENHA, @NIVEL, @STATUS)");
            var cmd = new NpgsqlCommand(instrucao, conn);
            cmd.Parameters.AddWithValue("@NOME", user.Nome);
            cmd.Parameters.AddWithValue("@CPF", user.Cpf);
            cmd.Parameters.AddWithValue("@EMAIL", user.Email);
            cmd.Parameters.AddWithValue("@SENHA", user.Senha);
            cmd.Parameters.AddWithValue("@NIVEL", user.Nivel);
            cmd.Parameters.AddWithValue("@STATUS", user.Status);
            try
            {
                if (cmd.ExecuteNonQuery() != 1)
                {
                    Console.WriteLine("Algo não deu muito certo ao criar usuário.");
                    ok = false;
                }
            }
            catch (NpgsqlException e)
            {
                Console.WriteLine("Erro " + e);
                ok = false;
            }
            return ok;
        }
        internal Usuario FindUser(String cpf)
        {
            var instrucao = string.Format("select * from usuario where cpf =@CPF", conn);
            var cmd = new NpgsqlCommand(instrucao, conn);
            cmd.Parameters.AddWithValue("@CPF", cpf);
            try
            {
                using (NpgsqlDataReader rd = cmd.ExecuteReader())
                {
                    while (rd.Read())
                    {
                        var user = new Usuario();
                        user.Nome = rd[0].ToString().Trim();
                        user.Cpf = rd[1].ToString().Trim();
                        user.Email = rd[2].ToString().Trim();
                        user.Senha = rd[3].ToString().Trim();
                        user.Nivel = rd[4].ToString().Trim();
                        user.Status = Convert.ToBoolean(rd[5].ToString().Trim());
                        return user;
                    }
                }
            }
            catch (NpgsqlException e)
            {
                Console.WriteLine("Erro " + e);
            }
            return null;
        }

        internal bool EditUser(Usuario user)
        {
            bool ok = true;
            String instrucao;
            instrucao = string.Format("update usuario set nome = @NOME, email=@EMAIL, nivel=@NIVEL, cpf = @CPF, status = @STATUS where cpf = @CPF");
            var cmd = new NpgsqlCommand(instrucao, conn);
            cmd.Parameters.AddWithValue("@NOME", user.Nome);
            cmd.Parameters.AddWithValue("@NIVEL", user.Nivel);
            cmd.Parameters.AddWithValue("@CPF", user.Cpf);
            cmd.Parameters.AddWithValue("@STATUS", user.Status);
            cmd.Parameters.AddWithValue("@EMAIL", user.Email);
            try
            {
                if (cmd.ExecuteNonQuery() != 1)
                {
                    Console.WriteLine("Algo não deu muito certo ao editar usuário.");
                    ok = false;
                }
            }
            catch (NpgsqlException e)
            {
                Console.WriteLine("Erro " + e);
                ok = false;
            }
            return ok;
        }

        internal bool InativaMatricula(Matricula matricula)
        {
            bool ok = true;
            String instrucao;
            instrucao = string.Format("update matricula set status=false, dtrescisao=@DTRESCISAO, motivorescisao=@MOTIVO where pkmatricula = @PKMATRICULA");
            var cmd = new NpgsqlCommand(instrucao, conn);
            cmd.Parameters.AddWithValue("@DTRESCISAO", DateTime.Now);
            cmd.Parameters.AddWithValue("@MOTIVO", matricula.MotivoRescisao.Trim());
            cmd.Parameters.AddWithValue("@PKMATRICULA", matricula.PkMatricula.Trim());

            try
            {
                if (cmd.ExecuteNonQuery() != 1)
                {
                    Console.WriteLine("Algo não deu muito certo ao inativar matrícula.");
                    ok = false;
                }
            }
            catch (NpgsqlException e)
            {
                Console.WriteLine("Erro " + e);
                ok = false;
            }
            return ok;
        }
        internal bool ResetPassword(Usuario user)
        {
            bool ok = true;
            String instrucao;
            instrucao = string.Format("update usuario set senha =@SENHA where cpf = @CPF");
            var cmd = new NpgsqlCommand(instrucao, conn);
            cmd.Parameters.AddWithValue("@SENHA", user.Senha);
            cmd.Parameters.AddWithValue("@CPF", user.Cpf);
            try
            {
                if (cmd.ExecuteNonQuery() != 1)
                {
                    Console.WriteLine("Algo não deu muito certo ao resetar password.");
                    ok = false;
                }
            }
            catch (NpgsqlException e)
            {
                Console.WriteLine("Erro " + e);
                ok = false;
            }
            return ok;
        }
        internal Usuario[] ListAllUsers()
        {
            var listUsuario = new List<Usuario>();
            var cmd = new NpgsqlCommand("select * from usuario", conn);
            try
            {
                using (NpgsqlDataReader rd = cmd.ExecuteReader())
                {
                    while (rd.Read())
                    {
                        var user = new Usuario();
                        user.Nome = rd[0].ToString();
                        user.Cpf = rd[1].ToString();
                        user.Email = rd[2].ToString();
                        user.Senha = rd[3].ToString();
                        user.Nivel = rd[4].ToString();
                        user.Status = Convert.ToBoolean(rd[5].ToString());

                        listUsuario.Add(user);
                    }
                }
            }
            catch (NpgsqlException e)
            {
                Console.WriteLine("Erro " + e);
            }
            return listUsuario.ToArray();
        }
        internal bool NewPessoa(Pessoa pessoa)
        {
            bool ok = true;
            var instrucao = string.Format("insert into pessoa (cpf,nome,sexo,dtnasc,dtcad,email,status,celular,pkpessoa) values (@CPF,@NOME,@SEXO,@DTNASC,@DTCAD,@EMAIL,@STATUS,@CELULAR,@PKPESSOA)");
            var cmd = new NpgsqlCommand(instrucao, conn);
            cmd.Parameters.AddWithValue("@CPF", pessoa.Cpf);
            cmd.Parameters.AddWithValue("@NOME", pessoa.Nome);
            cmd.Parameters.AddWithValue("@SEXO", pessoa.Sexo);
            cmd.Parameters.AddWithValue("@DTNASC", pessoa.DtNasc);
            cmd.Parameters.AddWithValue("@DTCAD", DateTime.Now);
            cmd.Parameters.AddWithValue("@EMAIL", pessoa.Email);
            cmd.Parameters.AddWithValue("@STATUS", pessoa.Status);
            cmd.Parameters.AddWithValue("@CELULAR", pessoa.Celular);
            cmd.Parameters.AddWithValue("@PKPESSOA", pessoa.PkPessoa);

            try
            {
                if (cmd.ExecuteNonQuery() != 1)
                {
                    Console.WriteLine("Algo não deu muito certoao criar usuário.");
                    ok = false;
                }
            }
            catch (NpgsqlException e)
            {
                Console.WriteLine("Erro " + e);
                ok = false;
            }
            return ok;
        }
        internal Pessoa FindPessoaCpf(String cpf)
        {
            string instrucao = string.Format("select * from pessoa where cpf =@CPF");
            var cmd = new NpgsqlCommand(instrucao, conn);
            cmd.Parameters.AddWithValue("@CPF", cpf);
            try
            {
                using (NpgsqlDataReader rd = cmd.ExecuteReader())
                {
                    while (rd.Read())
                    {
                        var pessoa = new Pessoa();
                        pessoa.Cpf = rd[0].ToString();
                        pessoa.Nome = rd[1].ToString();
                        pessoa.Sexo = rd[2].ToString();
                        pessoa.DtNasc = DateTime.Parse(rd[3].ToString());
                        pessoa.DtCad = DateTime.Parse(rd[4].ToString());
                        pessoa.Email = rd[5].ToString();
                        pessoa.Status = Convert.ToBoolean(rd[6].ToString());
                        pessoa.Celular = rd[7].ToString();
                        pessoa.PkPessoa = rd[8].ToString();
                        return pessoa;
                    }
                }
            }
            catch (NpgsqlException e)
            {
                Console.WriteLine("Erro " + e);
            }
            return null;
        }
        internal Pessoa FindPessoaPk(String pkPessoa)
        {
            var pessoa = new Pessoa();
            String instrucao = "select * from pessoa where pkpessoa = @PKPESSOA";
            var cmd = new NpgsqlCommand(instrucao, conn);
            cmd.Parameters.AddWithValue("@PKPESSOA", pkPessoa.Trim());
            try
            {
                using (NpgsqlDataReader rd = cmd.ExecuteReader())
                {
                    while (rd.Read())
                    {

                        pessoa.Cpf = rd[0].ToString();
                        pessoa.Nome = rd[1].ToString();
                        pessoa.Sexo = rd[2].ToString();
                        pessoa.DtNasc = DateTime.Parse(rd[3].ToString());
                        pessoa.DtCad = DateTime.Parse(rd[4].ToString());
                        pessoa.Email = rd[5].ToString();
                        pessoa.Status = Convert.ToBoolean(rd[6].ToString());
                        pessoa.Celular = rd[7].ToString();
                        pessoa.PkPessoa = rd[8].ToString();


                    }
                }
            }
            catch (NpgsqlException e)
            {
                Console.WriteLine("Erro " + e);
                return null;
            }
            return pessoa;
        }

        internal bool VerificaCpfEditado(Pessoa pessoa)//Verifica se CPF da pessoa editada não está em outro cadastro
        {
            if (!string.IsNullOrEmpty(pessoa.Cpf.Trim()))
            {
                string instrucao = "select * from pessoa where cpf =@CPF and pkpessoa!=@PKPESSOA";
                var cmd = new NpgsqlCommand(instrucao, conn);
                cmd.Parameters.AddWithValue("@CPF", pessoa.Cpf);
                cmd.Parameters.AddWithValue("@PKPESSOA", pessoa.PkPessoa.Trim());
                try
                {
                    using (NpgsqlDataReader rd = cmd.ExecuteReader())
                    {
                        if (rd.Read())
                        {
                            return true;
                        }
                        else
                            return false;
                    }
                }
                catch (NpgsqlException e)
                {
                    Console.WriteLine("Erro " + e);
                }
                return true;
            }
            else
                return false;
        }

        internal bool NewEndereco(Endereco endereco)
        {
            bool ok = true;
            var instrucao = string.Format("insert into endereco(logradouro, numero, bairro, complemento, uf, cidade, cep, fkpessoa, pkendereco) values (@LOGRADOURO, @NUMERO, @BAIRRO, @COMPLEMENTO, @UF, @CIDADE, @CEP, @FKPESSOA, @PKENDERECO)");
            var cmd = new NpgsqlCommand(instrucao, conn);
            cmd.Parameters.AddWithValue("@LOGRADOURO", endereco.Logradouro);
            cmd.Parameters.AddWithValue("@NUMERO", endereco.Numero);
            cmd.Parameters.AddWithValue("@BAIRRO", endereco.Bairro);
            cmd.Parameters.AddWithValue("@COMPLEMENTO", endereco.Complemento);
            cmd.Parameters.AddWithValue("@UF", endereco.Uf);
            cmd.Parameters.AddWithValue("@CIDADE", endereco.Cidade);
            cmd.Parameters.AddWithValue("@CEP", endereco.Cep);
            cmd.Parameters.AddWithValue("@FKPESSOA", endereco.FkPessoa);
            cmd.Parameters.AddWithValue("@PKENDERECO", endereco.PkEndereco);
            try
            {
                if (cmd.ExecuteNonQuery() != 1)
                {
                    Console.WriteLine("Algo não deu muito certo ao criar endereço.");
                    ok = false;
                }
            }
            catch (NpgsqlException e)
            {
                Console.WriteLine("Erro " + e);
                ok = false;
            }
            return ok;
        }
        internal Pessoa[] ListAllPessoas()
        {
            var listPessoas = new List<Pessoa>();
            var cmd = new NpgsqlCommand("select * from pessoa", conn);
            try
            {
                using (NpgsqlDataReader rd = cmd.ExecuteReader())
                {
                    while (rd.Read())
                    {
                        listPessoas.Add(PopulaPessoa(rd));
                    }
                }
            }
            catch (NpgsqlException e)
            {
                Console.WriteLine("Erro " + e);
            }
            return listPessoas.ToArray();
        }


        internal Aniversariante[] FiltraAniversariantes(DiaMesAniversario diaMes)
        {
            var listAnivesariantes = new List<Aniversariante>();
            string instrucao = "";
            if (diaMes.Dia > 0)
            {
                instrucao = string.Format("select nome, dtnasc, email, logradouro, numero, bairro, complemento, uf, cidade, cep from pessoa inner join endereco on pkpessoa=fkpessoa where status=true and Extract(Month From dtnasc)=@MES and Extract(Day From dtnasc)=@DIA");
            }
            else
            {
                instrucao = string.Format("select nome, dtnasc, email, logradouro, numero, bairro, complemento, uf, cidade, cep from pessoa inner join endereco on pkpessoa=fkpessoa where status=true and Extract(Month From dtnasc)=@MES");
            }
            var cmd = new NpgsqlCommand(instrucao, conn);
            cmd.Parameters.AddWithValue("@MES", diaMes.Mes);
            cmd.Parameters.AddWithValue("@DIA", diaMes.Dia);
            try
            {
                using (NpgsqlDataReader rd = cmd.ExecuteReader())
                {
                    while (rd.Read())
                    {
                        const int NOME = 0;
                        const int DTNASC = 1;
                        const int EMAIL = 2;
                        const int LOGRADOURO = 3;
                        const int NUMERO = 4;
                        const int BAIRRO = 5;
                        const int COMPLEMENTO = 6;
                        const int UF = 7;
                        const int CIDADE = 8;
                        const int CEP = 9;

                        Aniversariante aniversariante = new Aniversariante
                        {
                            Nome = rd[NOME].ToString(),
                            Dtnasc = DateTime.Parse(rd[DTNASC].ToString()),
                            Email = rd[EMAIL].ToString(),
                            Logradouro = rd[LOGRADOURO].ToString(),
                            Numero = Int32.Parse(rd[NUMERO].ToString()),
                            Bairro = rd[BAIRRO].ToString(),
                            Complemento = rd[COMPLEMENTO].ToString(),
                            Uf = rd[UF].ToString(),
                            Cidade = rd[CIDADE].ToString(),
                            Cep = Int32.Parse(rd[CEP].ToString()),
                            Gerar = true,
                        };

                        listAnivesariantes.Add(aniversariante);
                    }
                }

            }
            catch (NpgsqlException e)
            {
                Console.WriteLine("Erro " + e);
            }
            return listAnivesariantes.ToArray();
        }


        internal PartEvento[] ListPartEvento(EventoMusical eventoMusical)
        {
            List<string> listSavePkPessoas = new List<string>();
            var listPartEvento = new List<PartEvento>();
            string instrucao = string.Format("select * from partevento where fkeventomusical=@PKEVENTO");

            var cmd = new NpgsqlCommand(instrucao, conn);
            cmd.Parameters.AddWithValue("@PKEVENTO", eventoMusical.PkEventoMusical.Trim());
            try
            {
                using (NpgsqlDataReader rd = cmd.ExecuteReader())
                {
                    while (rd.Read())
                    {
                        const int PKPARTEVENTO = 0;
                        const int FKPESSOA = 1;
                        //const int FKEVENTOMUSICAL = 2;
                        const int DESCRICAO = 3;


                        PartEvento partEvento = new PartEvento
                        {
                            Descricao = rd[DESCRICAO].ToString().Trim(),
                            PkPartEvento = rd[PKPARTEVENTO].ToString().Trim(),
                            Evento = eventoMusical,
                        };

                        listSavePkPessoas.Add(rd[FKPESSOA].ToString().Trim());

                        listPartEvento.Add(partEvento);
                    }
                }

            }
            catch (NpgsqlException e)
            {
                Console.WriteLine("Erro " + e);
            }

            for (int i = 0; i < listPartEvento.Count; ++i)
            {
                listPartEvento[i].Participante = FindPessoaPk(listSavePkPessoas[i]);
                listPartEvento[i].NomeParticipante = listPartEvento[i].Participante.Nome.Trim();
            }
            return listPartEvento.ToArray();
        }


        internal AlunosDoDia[] ListAlunosDoDia(Professor professor)
        {
            var listAlunosDoDia = new List<AlunosDoDia>();
            var instrucao = string.Format("select * from horario_marcado where fkprofessor=@PKPESSOA and diasemana=@DIASEMANA order by horainicio");
            List<string> listStrMatriculas = new List<string>();
            var cmd = new NpgsqlCommand(instrucao, conn);
            cmd.Parameters.AddWithValue("@PKPESSOA", professor.PkPessoa.Trim());
            cmd.Parameters.AddWithValue("DIASEMANA", ((int)DateTime.Now.DayOfWeek) + 1);
            try
            {
                using (NpgsqlDataReader rd = cmd.ExecuteReader())
                {
                    while (rd.Read())
                    {
                        //const int DIASEMANA = 0;
                        //const int FKPROFESSOR = 1;
                        const int HORAINICIO = 2;
                        const int REPOSICAO = 3;
                        const int FKMATRICULA = 4;
                        AlunosDoDia alunosDoDia = new AlunosDoDia()
                        {
                            HoraInicio = Convert.ToDateTime(rd[HORAINICIO].ToString()),
                            Reposicao = Convert.ToBoolean(rd[REPOSICAO].ToString()),
                        };
                        listAlunosDoDia.Add(alunosDoDia);
                        listStrMatriculas.Add(rd[FKMATRICULA].ToString());
                    }
                }
            }
            catch (NpgsqlException e)
            {
                Console.WriteLine("Erro " + e);
            }

            for (int i = 0; i < listAlunosDoDia.Count; ++i)
            {
                var matriculaListItem = new MatriculaListItem();
                matriculaListItem.PkMatricula = listStrMatriculas[i];//Atribuindo chave da matrícula para popular matrícula referente ao aluno deste horário.
                var matricula = new Matricula();
                matricula = FindMatricula(matriculaListItem);//Atribuindo matrícula ao objeto.
                listAlunosDoDia[i].MatriculaAluno = matricula;
                string strFoto = "";
                var manager = new File.Manager();
                strFoto = manager.FindFoto(matricula.Aluno);
                listAlunosDoDia[i].StrFoto = strFoto;

                if (ContaAtividade(matricula) > 0)
                {
                    listAlunosDoDia[i].Atividade = true;
                }
                var falta = new Falta();
                falta.MatriculaAluno = matricula;
                falta.Data = DateTime.Now;
                if (ContaFalta(falta) > 0)
                {
                    listAlunosDoDia[i].Falta = true;
                }
            }
            return listAlunosDoDia.ToArray();
        }


        internal Endereco FindEndereco(Pessoa pessoa)
        {
            var instrucao = "select * from endereco where fkpessoa =@FKPESSOA";
            var cmd = new NpgsqlCommand(instrucao, conn);
            cmd.Parameters.AddWithValue("@FKPESSOA", pessoa.PkPessoa.Trim());
            try
            {
                using (NpgsqlDataReader rd = cmd.ExecuteReader())
                {
                    while (rd.Read())
                    {
                        var endereco = new Endereco();
                        endereco.Logradouro = rd[0].ToString();
                        endereco.Numero = Int32.Parse(rd[1].ToString());
                        endereco.Bairro = rd[2].ToString();
                        endereco.Complemento = rd[3].ToString();
                        endereco.Uf = rd[4].ToString();
                        endereco.Cidade = rd[5].ToString();
                        endereco.Cep = Int32.Parse(rd[6].ToString());
                        return endereco;
                    }
                }

            }
            catch (NpgsqlException e)
            {
                Console.WriteLine("Erro " + e);
            }
            return null;
        }
        internal Pessoa[] ListFiltraPessoas(Pessoa pessoa)
        {
            var listPessoas = new List<Pessoa>();
            String instrucao = "select * from pessoa where nome iLIKE @NOME and cpf iLIKE @CPF and status=@STATUS order by nome asc";
            var cmd = new NpgsqlCommand(instrucao, conn);
            cmd.Parameters.AddWithValue("@NOME", "%" + pessoa.Nome.Trim() + "%");
            cmd.Parameters.AddWithValue("@CPF", "%" + pessoa.Cpf + "%");
            cmd.Parameters.AddWithValue("@STATUS", pessoa.Status);
            try
            {
                using (NpgsqlDataReader rd = cmd.ExecuteReader())
                {
                    while (rd.Read())
                    {
                        listPessoas.Add(PopulaPessoa(rd));
                    }
                }
            }
            catch (NpgsqlException e)
            {
                Console.WriteLine("Erro " + e);
            }
            return listPessoas.ToArray();
        }


        internal Pessoa[] ListFiltraPessoas18(Pessoa pessoa)
        {
            var listPessoas = new List<Pessoa>();
            String instrucao = "select * from pessoa where nome iLIKE @NOME and cpf iLIKE @CPF and status=@STATUS and dtnasc <=@DATA order by nome asc";
            var cmd = new NpgsqlCommand(instrucao, conn);
            cmd.Parameters.AddWithValue("@NOME", "%" + pessoa.Nome + "%");
            cmd.Parameters.AddWithValue("@CPF", "%" + pessoa.Cpf + "%");
            cmd.Parameters.AddWithValue("@STATUS", pessoa.Status);
            cmd.Parameters.AddWithValue("@DATA", DateTime.Now.AddYears(-18));
            try
            {
                using (NpgsqlDataReader rd = cmd.ExecuteReader())
                {
                    while (rd.Read())
                    {
                        listPessoas.Add(PopulaPessoa(rd));
                    }
                }
            }
            catch (NpgsqlException e)
            {
                Console.WriteLine("Erro " + e);
            }
            return listPessoas.ToArray();
        }


        internal Pessoa[] ListFiltraAlunos(Pessoa pessoa)
        {
            var listPessoas = new List<Pessoa>();
            String instrucao = "select distinct (pessoa).* from pessoa inner join matricula on fkaluno=pkpessoa where nome iLIKE @NOME and cpf iLIKE @CPF and status=@STATUS order by nome asc";
            var cmd = new NpgsqlCommand(instrucao, conn);
            cmd.Parameters.AddWithValue("@NOME", "%" + pessoa.Nome + "%");
            cmd.Parameters.AddWithValue("@CPF", "%" + pessoa.Cpf + "%");
            cmd.Parameters.AddWithValue("@STATUS", pessoa.Status);

            try
            {
                using (NpgsqlDataReader rd = cmd.ExecuteReader())
                {
                    while (rd.Read())
                    {
                        listPessoas.Add(PopulaPessoa(rd));
                    }
                }

            }
            catch (NpgsqlException e)
            {
                Console.WriteLine("Erro " + e);
            }
            return listPessoas.ToArray();
        }

        internal Pessoa[] ListFiltraProfessores(Pessoa pessoa)
        {
            var listPessoas = new List<Pessoa>();
            String instrucao = "select distinct (pessoa).* from pessoa inner join comissao on fkprofessor=pkpessoa where nome iLIKE @NOME and cpf iLIKE @CPF and status=@STATUS order by nome asc";
            var cmd = new NpgsqlCommand(instrucao, conn);
            cmd.Parameters.AddWithValue("@NOME", "%" + pessoa.Nome + "%");
            cmd.Parameters.AddWithValue("@CPF", "%" + pessoa.Cpf + "%");
            cmd.Parameters.AddWithValue("@STATUS", pessoa.Status);
            try
            {
                using (NpgsqlDataReader rd = cmd.ExecuteReader())
                {
                    while (rd.Read())
                    {
                        listPessoas.Add(PopulaPessoa(rd));
                    }
                }

            }
            catch (NpgsqlException e)
            {
                Console.WriteLine("Erro " + e);
            }
            return listPessoas.ToArray();
        }


        internal Pessoa PopulaPessoa(NpgsqlDataReader rd)
        {
            var pessoa = new Pessoa
            {
                Cpf = rd[0].ToString(),
                Nome = rd[1].ToString(),
                Sexo = rd[2].ToString(),
                DtNasc = Convert.ToDateTime(rd[3].ToString()),
                DtCad = Convert.ToDateTime(rd[4].ToString()),
                Email = rd[5].ToString(),
                Status = Convert.ToBoolean(rd[6].ToString()),
                Celular = rd[7].ToString(),
                PkPessoa = rd[8].ToString()
            };

            return pessoa;
        }

        internal bool NewResponsavel(Responsavel responsavel)
        {
            bool ok = true;
            var instrucao = string.Format("insert into responsavel (pkresponsavel, fkmenor, fkresponsavel) values (@PKRESPONSAVEL, @FKMENOR, @FKRESPONSAVEL)");
            var cmd = new NpgsqlCommand(instrucao, conn);
            cmd.Parameters.AddWithValue("@PKRESPONSAVEL", responsavel.PkResponsavel);
            cmd.Parameters.AddWithValue("@FKMENOR", responsavel.FkMenor);
            cmd.Parameters.AddWithValue("@FKRESPONSAVEL", responsavel.FkResponsavel);

            try
            {
                if (cmd.ExecuteNonQuery() != 1)
                {
                    Console.WriteLine("Algo não deu muito certo ao criar responsável.");
                    ok = false;
                }
            }
            catch (NpgsqlException e)
            {
                Console.WriteLine("Erro " + e);
                ok = false;
            }
            return ok;
        }


        internal Pessoa FindResponsavel(Pessoa pessoa)
        {
            var instrucao = string.Format("select * from pessoa  inner join responsavel on pkpessoa=fkresponsavel  where fkmenor=@PKPESSOA");
            var cmd = new NpgsqlCommand(instrucao, conn);
            cmd.Parameters.AddWithValue("@PKPESSOA", pessoa.PkPessoa.Trim());
            try
            {
                using (NpgsqlDataReader rd = cmd.ExecuteReader())
                {
                    while (rd.Read())
                    {
                        return PopulaPessoa(rd);
                    }
                }
            }
            catch (NpgsqlException e)
            {
                Console.WriteLine("Erro " + e);
            }
            return null;
        }

        internal bool DelResponsavel(Pessoa pessoa)
        {
            bool ok = true;
            var instrucao = string.Format("delete from responsavel where fkmenor=@PKPESSOA");
            var cmd = new NpgsqlCommand(instrucao, conn);
            cmd.Parameters.AddWithValue("@PKPESSOA", pessoa.PkPessoa);
            try
            {
                if (cmd.ExecuteNonQuery() != 1)
                {
                    Console.WriteLine("Algo não deu muito certo ao excluir responsável.");
                    ok = false;
                }
            }
            catch (NpgsqlException e)
            {
                Console.WriteLine("Erro " + e);
                ok = false;
            }
            return ok;
        }
        internal bool EditEndereco(Endereco endereco)
        {
            var ok = true;
            var instrucao = string.Format("update endereco set logradouro= @LOGRADOURO, numero= @NUMERO, bairro= @BAIRRO, complemento= @COMPLEMENTO, UF= @UF, cidade= @CIDADE, cep= @CEP where fkpessoa = @FKPESSOA");
            var cmd = new NpgsqlCommand(instrucao, conn);
            cmd.Parameters.AddWithValue("@LOGRADOURO", endereco.Logradouro);
            cmd.Parameters.AddWithValue("@NUMERO", endereco.Numero);
            cmd.Parameters.AddWithValue("@BAIRRO", endereco.Bairro);
            cmd.Parameters.AddWithValue("@COMPLEMENTO", endereco.Complemento);
            cmd.Parameters.AddWithValue("@UF", endereco.Uf);
            cmd.Parameters.AddWithValue("@CIDADE", endereco.Cidade);
            cmd.Parameters.AddWithValue("@CEP", endereco.Cep);
            cmd.Parameters.AddWithValue("@FKPESSOA", endereco.FkPessoa.Trim());
            try
            {
                if (cmd.ExecuteNonQuery() != 1)
                {
                    Console.WriteLine("Algo não deu muito certo ao editar endereço.");
                    ok = false;
                }
            }
            catch (NpgsqlException e)
            {
                Console.WriteLine("Erro " + e);
                ok = false;
            }
            return ok;

        }
        internal bool EditPessoa(Pessoa pessoa)
        {
            bool ok = true;
            if (VerificaCpfEditado(pessoa))
            {
                ok = false;
            }
            else
            {
                var instrucao = string.Format("update pessoa set cpf= @CPF, nome= @NOME, sexo= @SEXO, dtnasc= @DTNASC, email= @EMAIL, status= @STATUS, celular= @CELULAR where pkpessoa= @PKPESSOA");
                var cmd = new NpgsqlCommand(instrucao, conn);
                cmd.Parameters.AddWithValue("@CPF", pessoa.Cpf.Trim());
                cmd.Parameters.AddWithValue("@NOME", pessoa.Nome.Trim());
                cmd.Parameters.AddWithValue("@SEXO", pessoa.Sexo.Trim());
                cmd.Parameters.AddWithValue("@DTNASC", pessoa.DtNasc.Date);
                cmd.Parameters.AddWithValue("@EMAIL", pessoa.Email.Trim());
                cmd.Parameters.AddWithValue("@STATUS", pessoa.Status);
                cmd.Parameters.AddWithValue("@CELULAR", pessoa.Celular.Trim());
                cmd.Parameters.AddWithValue("@PKPESSOA", pessoa.PkPessoa.Trim());
                try
                {
                    if (cmd.ExecuteNonQuery() != 1)
                    {
                        Console.WriteLine("Algo não deu muito certo ao editar pessoa.");
                        ok = false;
                    }
                }
                catch (NpgsqlException e)
                {
                    Console.WriteLine("Erro " + e);
                    ok = false;
                }
            }
            return ok;
        }

        internal bool EditResponsavel(Responsavel responsavel)
        {
            bool ok = true;
            var instrucao = string.Format("update responsavel set fkresponsavel=@FKRESPONSAVEL where fkmenor=@FKMENOR");
            var cmd = new NpgsqlCommand(instrucao, conn);
            cmd.Parameters.AddWithValue("@FKRESPONSAVEL", responsavel.FkResponsavel.Trim());
            cmd.Parameters.AddWithValue("@FKMENOR", responsavel.FkMenor.Trim());
            try
            {
                if (cmd.ExecuteNonQuery() != 1)
                {
                    Console.WriteLine("Algo não deu muito certo ao editar responsável.");
                    ok = false;
                }
            }
            catch (NpgsqlException e)
            {
                Console.WriteLine("Erro " + e);
                ok = false;
            }
            return ok;
        }

        internal bool NewCurso(Curso curso)
        {
            bool ok = true;
            var instrucao = string.Format("insert into curso (pkcurso, instrumento, mensalidade, status, frequencia) values (@PKCURSO, @INSTRUMENTO, @MENSALIDADE, true, @FREQUENCIA)");
            var cmd = new NpgsqlCommand(instrucao, conn);
            cmd.Parameters.AddWithValue("@PKCURSO", curso.PkCurso.Trim());
            cmd.Parameters.AddWithValue("@INSTRUMENTO", curso.Instrumento.Trim());
            cmd.Parameters.AddWithValue("@MENSALIDADE", curso.Mensalidade);
            cmd.Parameters.AddWithValue("@FREQUENCIA", curso.Frequencia);
            try
            {
                if (cmd.ExecuteNonQuery() != 1)
                {
                    Console.WriteLine("Algo não deu muito certo ao criar curso.");
                    ok = false;
                }
            }
            catch (NpgsqlException e)
            {
                Console.WriteLine("Erro " + e);
                ok = false;
            }
            return ok;
        }


        internal Curso FindCurso(String PkCurso)
        {
            Curso curso = new Curso();
            string instrucao = "select * from curso where pkcurso = @PKCURSO";
            var cmd = new NpgsqlCommand(instrucao, conn);
            cmd.Parameters.AddWithValue("@PKCURSO", PkCurso.Trim());
            try
            {
                using (NpgsqlDataReader rd = cmd.ExecuteReader())
                {
                    while (rd.Read())
                    {
                        const int pkCurso = 0;
                        const int instrumento = 1;
                        const int mensalidade = 2;
                        const int status = 3;
                        const int frequencia = 4;

                        curso.PkCurso = rd[pkCurso].ToString();
                        curso.Instrumento = rd[instrumento].ToString();
                        curso.Mensalidade = Double.Parse(rd[mensalidade].ToString().Replace(".", ",").Trim());
                        curso.Status = Convert.ToBoolean(rd[status].ToString());
                        curso.Frequencia = Int32.Parse(rd[frequencia].ToString());
                    }
                }
            }
            catch (NpgsqlException e)
            {
                Console.WriteLine("Erro " + e);
            }
            return curso;
        }

        internal Curso[] ListAllCursos()
        {
            var listCursos = new List<Curso>();
            var cmd = new NpgsqlCommand("select * from curso order by instrumento", conn);
            try
            {
                using (NpgsqlDataReader rd = cmd.ExecuteReader())
                {
                    while (rd.Read())
                    {
                        var curso = new Curso();
                        curso.PkCurso = rd[0].ToString();
                        curso.Instrumento = rd[1].ToString();
                        curso.Mensalidade = Double.Parse(rd[2].ToString().Replace(".", ",").Trim());
                        curso.Status = Convert.ToBoolean(rd[3].ToString());
                        curso.Frequencia = Int32.Parse(rd[4].ToString());

                        listCursos.Add(curso);
                    }
                }
            }
            catch (NpgsqlException e)
            {
                Console.WriteLine("Erro " + e);
            }
            return listCursos.ToArray();
        }


        internal bool EditCurso(Curso curso)
        {
            bool ok = true;
            String instrucao;
            instrucao = string.Format("update curso set instrumento = @INSTRUMENTO, mensalidade = @MENSALIDADE, status = @STATUS, frequencia = @FREQUENCIA where pkcurso = @PKCURSO");
            var cmd = new NpgsqlCommand(instrucao, conn);
            cmd.Parameters.AddWithValue("@PKCURSO", curso.PkCurso.Trim());
            cmd.Parameters.AddWithValue("@INSTRUMENTO", curso.Instrumento.Trim());
            cmd.Parameters.AddWithValue("@MENSALIDADE", curso.Mensalidade);
            cmd.Parameters.AddWithValue("@FREQUENCIA", curso.Frequencia);
            cmd.Parameters.AddWithValue("@STATUS", curso.Status);
            try
            {
                if (cmd.ExecuteNonQuery() != 1)
                {
                    Console.WriteLine("Algo não deu muito certo ao editar curso.");
                    ok = false;
                }
            }
            catch (NpgsqlException e)
            {
                Console.WriteLine("Erro " + e);
                ok = false;
            }
            return ok;
        }

        internal bool NewComissao(Comissao comissao)
        {
            bool ok = true;
            var instrucao = string.Format("insert into comissao (pkcomissao, fkprofessor, comissao, assistente) values (@PKCOMISSAO, @FKPROFESSOR, @COMISSAO, @ASSISTENTE)");
            var cmd = new NpgsqlCommand(instrucao, conn);
            cmd.Parameters.AddWithValue("@PKCOMISSAO", comissao.PkComissao.Trim());
            cmd.Parameters.AddWithValue("@FKPROFESSOR", comissao.FkProfessor.Trim());
            cmd.Parameters.AddWithValue("@COMISSAO", comissao.VlrComissao);
            cmd.Parameters.AddWithValue("@ASSISTENTE", comissao.Assistente);
            try
            {
                if (cmd.ExecuteNonQuery() != 1)
                {
                    Console.WriteLine("Algo não deu muito certo ao criar comissão.");
                    ok = false;
                }
            }
            catch (NpgsqlException e)
            {
                Console.WriteLine("Erro " + e);
                ok = false;
            }
            return ok;
        }

        internal bool NewHorarioDisponivel(HorarioDisponivel horarioDisponivel)
        {
            var ok = true;
            var instrucao = string.Format("insert into horario_disponivel (fkprofessor, diasemana, horainicio) values (@FKPROFESSOR, @DIASEMANA, @HORAINICIO)");
            var cmd = new NpgsqlCommand(instrucao, conn);
            cmd.Parameters.AddWithValue("@FKPROFESSOR", horarioDisponivel.FkProfessor.Trim());
            cmd.Parameters.AddWithValue("@DIASEMANA", horarioDisponivel.DiaSemana);
            cmd.Parameters.AddWithValue("@HORAINICIO", horarioDisponivel.HoraInicio);
            try
            {
                if (cmd.ExecuteNonQuery() != 1)
                {
                    Console.WriteLine("Algo não deu muito certo ao criar horário disponível.");
                    ok = false;
                }
            }
            catch (NpgsqlException e)
            {
                Console.WriteLine("Erro " + e);
                ok = false;
            }
            return ok;
        }

        internal bool NewHorarioMarcado(List<HorarioMarcado> listHorarioMarcado)
        {
            bool ok = true;
            for (int i = 0; i < listHorarioMarcado.Count; ++i)
            {
                var instrucao = string.Format("insert into horario_marcado (diasemana, fkprofessor,horainicio, reposicao, fkmatricula) values (@DIASEMANA, @FKPROFESSOR,@HORAINICIO, @REPOSICAO, @FKMATRICULA)");
                var cmd = new NpgsqlCommand(instrucao, conn);
                cmd.Parameters.AddWithValue("@DIASEMANA", listHorarioMarcado[i].DiaSemana);
                cmd.Parameters.AddWithValue("@FKPROFESSOR", listHorarioMarcado[i].FkProfessor.Trim());
                cmd.Parameters.AddWithValue("@HORAINICIO", listHorarioMarcado[i].HoraInicio);
                cmd.Parameters.AddWithValue("@REPOSICAO", listHorarioMarcado[i].Reposicao);
                cmd.Parameters.AddWithValue("@FKMATRICULA", listHorarioMarcado[i].matriculaAluno.PkMatricula);
                try
                {
                    if (cmd.ExecuteNonQuery() != 1)
                    {
                        Console.WriteLine("Algo não deu muito certo ao agendar horário");
                        ok = false;
                    }
                }
                catch (NpgsqlException e)
                {
                    Console.WriteLine("Erro " + e);
                    ok = false;
                }
            }
            return ok;
        }


        internal Professor[] ListAllProfessores()
        {
            var listProfessor = new List<Professor>();
            var instrucao = string.Format("select * from comissao inner join pessoa on fkprofessor=pkpessoa where pessoa.status=true order by pessoa.nome");
            var cmd = new NpgsqlCommand(instrucao, conn);
            try
            {
                using (NpgsqlDataReader rd = cmd.ExecuteReader())
                {
                    const int comissao = 2;
                    const int assistente = 3;
                    const int cpf = 4;
                    const int nome = 5;
                    const int sexo = 6;
                    const int dtnasc = 7;
                    const int dtcad = 8;
                    const int email = 9;
                    const int status = 10;
                    const int celular = 11;
                    const int pkpessoa = 12;

                    while (rd.Read())
                    {
                        var professor = new Professor
                        {
                            Cpf = rd[cpf].ToString(),
                            Nome = rd[nome].ToString(),
                            Sexo = rd[sexo].ToString(),
                            DtNasc = Convert.ToDateTime(rd[dtnasc].ToString()),
                            DtCad = Convert.ToDateTime(rd[dtcad].ToString()),
                            Email = rd[email].ToString(),
                            Status = Convert.ToBoolean(rd[status].ToString()),
                            Celular = rd[celular].ToString(),
                            PkPessoa = rd[pkpessoa].ToString(),
                            Comissao = Double.Parse(rd[comissao].ToString().Replace(".", ",").Trim()),
                            Assistente = Convert.ToBoolean(rd[assistente].ToString()),
                        };

                        listProfessor.Add(professor);
                    }
                }
            }
            catch (NpgsqlException e)
            {
                Console.WriteLine("Erro " + e);
            }
            return listProfessor.ToArray();
        }


        internal DiaHora[] ListAllHorarios(Professor professor)
        {
            var listDiaHora = new List<DiaHora>();

            string instrucao = "select * from horario_disponivel where fkprofessor=@PKPESSOA order by diasemana, horainicio";

            var horaAbertura = DateTime.MinValue.AddHours(7);//Horário de abertura da escola
            var horaEncerramento = DateTime.MinValue.AddHours(19);//Horário de encerramento da escola

            for (var i = horaAbertura; i <= horaEncerramento; i = i.AddMinutes(30))
            {
                var diaHora = new DiaHora
                {
                    Hora = i,
                    HoraStr = i.ToString("HH:mm")
                };

                var cmd = new NpgsqlCommand(instrucao, conn);
                cmd.Parameters.AddWithValue("@PKPESSOA", professor.PkPessoa.Trim());
                try
                {
                    using (NpgsqlDataReader rd = cmd.ExecuteReader())
                    {
                        const int DIA_SEMANA = 1;
                        const int HORA_INICIO = 2;
                        var horaBd = new DateTime();

                        while (rd.Read())
                        {
                            horaBd = Convert.ToDateTime(rd[HORA_INICIO].ToString());
                            switch (Int32.Parse(rd[DIA_SEMANA].ToString()))
                            {
                                case 2:
                                    if (i == horaBd)
                                    {
                                        diaHora.Segunda = true;
                                    }
                                    break;
                                case 3:
                                    if (i == horaBd)
                                    {
                                        diaHora.Terca = true;
                                    }
                                    break;
                                case 4:
                                    if (i == horaBd)
                                    {
                                        diaHora.Quarta = true;
                                    }
                                    break;
                                case 5:
                                    if (i == horaBd)
                                    {
                                        diaHora.Quinta = true;
                                    }
                                    break;
                                case 6:
                                    if (i == horaBd)
                                    {
                                        diaHora.Sexta = true;
                                    }
                                    break;
                            }
                        }
                    };
                }
                catch (NpgsqlException e)
                {
                    Console.WriteLine("Erro " + e);
                }
                listDiaHora.Add(diaHora);
            }

            return listDiaHora.ToArray();
        }

        internal DiaHoraAlunos[] ListAgendaProfessor(Professor professor)
        {
            var listDiaHoraAlunos = new List<DiaHoraAlunos>();

            string instrucao = "select pessoa.nome as aluno, diasemana, horainicio, horario_marcado.reposicao, curso.instrumento from pessoa inner join matricula on matricula.fkaluno= pessoa.pkpessoa inner join horario_marcado on fkmatricula=pkmatricula inner join curso on pkcurso=matricula.fkcurso where horario_marcado.fkprofessor=@PKPESSOA";
            var horaAbertura = DateTime.MinValue.AddHours(7);
            var horaEncerramento = DateTime.MinValue.AddHours(19);

            for (var i = horaAbertura; i <= horaEncerramento; i = i.AddMinutes(30))
            {
                var diaHoraAlunos = new DiaHoraAlunos
                {
                    Hora = i,
                    HoraStr = i.ToString("HH:mm"),
                    Segunda = "",
                    Terca = "",
                    Quarta = "",
                    Quinta = "",
                    Sexta = "",
                };

                var cmd = new NpgsqlCommand(instrucao, conn);
                cmd.Parameters.AddWithValue("@PKPESSOA", professor.PkPessoa.Trim());
                try
                {
                    using (NpgsqlDataReader rd = cmd.ExecuteReader())
                    {
                        const int NOME_ALUNO = 0;
                        const int DIA_SEMANA = 1;
                        const int HORA_INICIO = 2;
                        const int REPOSICAO = 3;
                        const int CURSO = 4;

                        bool PrimeiroNomeSegunda = true; //Variáveis para verificação, se é o primeiro nome atribuído na string, para não adicionar quebras de linhas sem necessidade
                        bool PrimeiroNomeTerca = true;
                        bool PrimeiroNomeQuarta = true;
                        bool PrimeiroNomeQuinta = true;
                        bool PrimeiroNomeSexta = true;

                        var horaBd = new DateTime();

                        while (rd.Read())
                        {
                            string NomeAluno = "";
                            horaBd = Convert.ToDateTime(rd[HORA_INICIO].ToString());
                            if (Convert.ToBoolean(rd[REPOSICAO].ToString()))//Verifica se é aula de reposição.
                            {
                                NomeAluno = rd[NOME_ALUNO].ToString().Trim() + "-Rep." + rd[CURSO].ToString().Trim();
                            }
                            else
                            {
                                NomeAluno = rd[NOME_ALUNO].ToString().Trim() + "-" + rd[CURSO].ToString().Trim();
                            }

                            switch (Int32.Parse(rd[DIA_SEMANA].ToString()))
                            {
                                case 2:
                                    if (i == horaBd)
                                    {
                                        if (PrimeiroNomeSegunda)
                                        {
                                            diaHoraAlunos.Segunda = NomeAluno;
                                        }
                                        else
                                        {
                                            diaHoraAlunos.Segunda = diaHoraAlunos.Segunda + "\n" + NomeAluno;
                                        }
                                        PrimeiroNomeSegunda = false;
                                    }
                                    break;
                                case 3:
                                    if (i == horaBd)
                                    {
                                        if (PrimeiroNomeTerca)
                                        {
                                            diaHoraAlunos.Terca = NomeAluno;
                                        }
                                        else
                                        {
                                            diaHoraAlunos.Terca = diaHoraAlunos.Terca + "\n" + NomeAluno;
                                        }
                                        PrimeiroNomeTerca = false;
                                    }
                                    break;
                                case 4:
                                    if (i == horaBd)
                                    {
                                        if (PrimeiroNomeQuarta)
                                        {
                                            diaHoraAlunos.Quarta = NomeAluno;
                                        }
                                        else
                                        {
                                            diaHoraAlunos.Quarta = diaHoraAlunos.Quarta + "\n" + NomeAluno;
                                        }
                                        PrimeiroNomeQuarta = false;
                                    }
                                    break;
                                case 5:
                                    if (i == horaBd)
                                    {
                                        if (PrimeiroNomeQuinta)
                                        {
                                            diaHoraAlunos.Quinta = NomeAluno;
                                        }
                                        else
                                        {
                                            diaHoraAlunos.Quinta = diaHoraAlunos.Quinta + "\n" + NomeAluno;
                                        }
                                        PrimeiroNomeQuinta = false;
                                    }
                                    break;
                                case 6:
                                    if (i == horaBd)
                                    {
                                        if (PrimeiroNomeSexta)
                                        {
                                            diaHoraAlunos.Sexta = NomeAluno;
                                        }
                                        else
                                        {
                                            diaHoraAlunos.Sexta = diaHoraAlunos.Sexta + "\n" + NomeAluno;
                                        }
                                        PrimeiroNomeSexta = false;
                                    }
                                    break;
                            }
                        }
                    };
                }
                catch (NpgsqlException e)
                {
                    Console.WriteLine("Erro " + e);
                }
                listDiaHoraAlunos.Add(diaHoraAlunos);
            }
            return listDiaHoraAlunos.ToArray();
        }

        internal DiaHoraAlunos[] ListAgendaAluno(Matricula matricula)
        {
            var listDiaHoraAlunos = new List<DiaHoraAlunos>();

            string instrucao = "select pessoa.nome as aluno, diasemana, horainicio, horario_marcado.reposicao, curso.instrumento from pessoa inner join matricula on matricula.fkaluno= pessoa.pkpessoa inner join horario_marcado on fkmatricula=pkmatricula inner join curso on pkcurso=matricula.fkcurso where horario_marcado.fkprofessor=@PKPESSOA and pkmatricula=@PKMATRICULA";
            var horaAbertura = DateTime.MinValue.AddHours(7);
            var horaEncerramento = DateTime.MinValue.AddHours(19);

            for (var i = horaAbertura; i <= horaEncerramento; i = i.AddMinutes(30))
            {
                var diaHoraAlunos = new DiaHoraAlunos
                {
                    Hora = i,
                    HoraStr = i.ToString("HH:mm"),
                    Segunda = "",
                    Terca = "",
                    Quarta = "",
                    Quinta = "",
                    Sexta = "",
                };

                var cmd = new NpgsqlCommand(instrucao, conn);
                cmd.Parameters.AddWithValue("@PKPESSOA", matricula.Professor.PkPessoa.Trim());
                cmd.Parameters.AddWithValue("@PKMATRICULA", matricula.PkMatricula.Trim());
                try
                {
                    using (NpgsqlDataReader rd = cmd.ExecuteReader())
                    {
                        const int NOME_ALUNO = 0;
                        const int DIA_SEMANA = 1;
                        const int HORA_INICIO = 2;
                        const int REPOSICAO = 3;
                        const int CURSO = 4;

                        bool PrimeiroNomeSegunda = true; //Variáveis para verificação, se é o primeiro nome atribuído na string, para não adicionar quebras de linhas sem necessidade
                        bool PrimeiroNomeTerca = true;
                        bool PrimeiroNomeQuarta = true;
                        bool PrimeiroNomeQuinta = true;
                        bool PrimeiroNomeSexta = true;

                        var horaBd = new DateTime();

                        while (rd.Read())
                        {
                            string NomeAluno = "";
                            horaBd = Convert.ToDateTime(rd[HORA_INICIO].ToString());
                            if (Convert.ToBoolean(rd[REPOSICAO].ToString()))//Verifica se é aula de reposição.
                            {
                                NomeAluno = rd[NOME_ALUNO].ToString().Trim() + "-Rep." + rd[CURSO].ToString().Trim();
                            }
                            else
                            {
                                NomeAluno = rd[NOME_ALUNO].ToString().Trim() + "-" + rd[CURSO].ToString().Trim();
                            }

                            switch (Int32.Parse(rd[DIA_SEMANA].ToString()))
                            {
                                case 2:
                                    if (i == horaBd)
                                    {
                                        if (PrimeiroNomeSegunda)
                                        {
                                            diaHoraAlunos.Segunda = NomeAluno;
                                        }
                                        else
                                        {
                                            diaHoraAlunos.Segunda = diaHoraAlunos.Segunda + "\n" + NomeAluno;
                                        }
                                        PrimeiroNomeSegunda = false;
                                    }
                                    break;
                                case 3:
                                    if (i == horaBd)
                                    {
                                        if (PrimeiroNomeTerca)
                                        {
                                            diaHoraAlunos.Terca = NomeAluno;
                                        }
                                        else
                                        {
                                            diaHoraAlunos.Terca = diaHoraAlunos.Terca + "\n" + NomeAluno;
                                        }
                                        PrimeiroNomeTerca = false;
                                    }
                                    break;
                                case 4:
                                    if (i == horaBd)
                                    {
                                        if (PrimeiroNomeQuarta)
                                        {
                                            diaHoraAlunos.Quarta = NomeAluno;
                                        }
                                        else
                                        {
                                            diaHoraAlunos.Quarta = diaHoraAlunos.Quarta + "\n" + NomeAluno;
                                        }
                                        PrimeiroNomeQuarta = false;
                                    }
                                    break;
                                case 5:
                                    if (i == horaBd)
                                    {
                                        if (PrimeiroNomeQuinta)
                                        {
                                            diaHoraAlunos.Quinta = NomeAluno;
                                        }
                                        else
                                        {
                                            diaHoraAlunos.Quinta = diaHoraAlunos.Quinta + "\n" + NomeAluno;
                                        }
                                        PrimeiroNomeQuinta = false;
                                    }
                                    break;
                                case 6:
                                    if (i == horaBd)
                                    {
                                        if (PrimeiroNomeSexta)
                                        {
                                            diaHoraAlunos.Sexta = NomeAluno;
                                        }
                                        else
                                        {
                                            diaHoraAlunos.Sexta = diaHoraAlunos.Sexta + "\n" + NomeAluno;
                                        }
                                        PrimeiroNomeSexta = false;
                                    }
                                    break;
                            }
                        }
                    };
                }
                catch (NpgsqlException e)
                {
                    Console.WriteLine("Erro " + e);
                }
                listDiaHoraAlunos.Add(diaHoraAlunos);
            }
            return listDiaHoraAlunos.ToArray();
        }


        internal bool EditComissao(Comissao comissao)
        {
            bool ok = true;
            var instrucao = string.Format("update comissao set comissao=@COMISSAO, assistente=@ASSISTENTE where fkprofessor=@FKPROFESSOR");
            var cmd = new NpgsqlCommand(instrucao, conn);
            cmd.Parameters.AddWithValue("@FKPROFESSOR", comissao.FkProfessor.Trim());
            cmd.Parameters.AddWithValue("@COMISSAO", comissao.VlrComissao);
            cmd.Parameters.AddWithValue("@ASSISTENTE", comissao.Assistente);
            try
            {
                if (cmd.ExecuteNonQuery() != 1)
                {
                    Console.WriteLine("Algo não deu muito certo ao editar comissão!");
                    ok = false;
                }
            }
            catch (NpgsqlException e)
            {
                Console.WriteLine("Erro " + e);
                ok = false;
            }
            return ok;
        }


        internal int VerificaHorarioMarcado(HorarioMarcado horarioMarcado)
        {
            int quantidade = 0;
            var instrucao = string.Format("select count (*) from horario_marcado where fkprofessor=@FKPROFESSOR and horainicio=@HORAINICIO and diasemana=@DIASEMANA and fkmatricula iLIKE @FKMATRICULA");
            var cmd = new NpgsqlCommand(instrucao, conn);
            cmd.Parameters.AddWithValue("@FKPROFESSOR", horarioMarcado.FkProfessor.Trim());
            cmd.Parameters.AddWithValue("@HORAINICIO", horarioMarcado.HoraInicio);
            cmd.Parameters.AddWithValue("@DIASEMANA", horarioMarcado.DiaSemana);
            cmd.Parameters.AddWithValue("@FKMATRICULA", "%" + horarioMarcado.matriculaAluno.PkMatricula + "%");

            try
            {
                using (NpgsqlDataReader rd = cmd.ExecuteReader())
                {
                    while (rd.Read())
                    {
                        quantidade = Int32.Parse(rd[0].ToString());
                    }
                }
            }
            catch (NpgsqlException e)
            {
                Console.WriteLine("Erro " + e);
            }
            return quantidade;
        }


        internal int VerificaHorarioMarcadoSemMusicalizacao(HorarioMarcado horarioMarcado)
        {
            int quantidade = 0;
            var instrucao = string.Format("select count (*) from horario_marcado inner join matricula on pkmatricula=horario_marcado.fkmatricula inner join curso on matricula.fkcurso=curso.pkcurso  where horario_marcado.fkprofessor=@FKPROFESSOR and horainicio=@HORAINICIO and diasemana=@DIASEMANA and instrumento not iLIKE '%Musicalização%'");
            var cmd = new NpgsqlCommand(instrucao, conn);
            cmd.Parameters.AddWithValue("@FKPROFESSOR", horarioMarcado.FkProfessor.Trim());
            cmd.Parameters.AddWithValue("@HORAINICIO", horarioMarcado.HoraInicio);
            cmd.Parameters.AddWithValue("@DIASEMANA", horarioMarcado.DiaSemana);
            try
            {
                using (NpgsqlDataReader rd = cmd.ExecuteReader())
                {
                    while (rd.Read())
                    {
                        quantidade = Int32.Parse(rd[0].ToString());
                    }
                }
            }
            catch (NpgsqlException e)
            {
                Console.WriteLine("Erro " + e);
            }
            return quantidade;
        }


        internal int VerificaHorarioComMusicalizacao(HorarioMarcado horarioMarcado)
        {
            int quantidade = 0;
            var instrucao = string.Format("select count (*) from horario_marcado inner join matricula on pkmatricula=horario_marcado.fkmatricula inner join curso on matricula.fkcurso=curso.pkcurso  where horario_marcado.fkprofessor=@FKPROFESSOR and horainicio=@HORAINICIO and diasemana=@DIASEMANA and instrumento iLIKE '%Musicalização%'");
            var cmd = new NpgsqlCommand(instrucao, conn);
            cmd.Parameters.AddWithValue("@FKPROFESSOR", horarioMarcado.FkProfessor.Trim());
            cmd.Parameters.AddWithValue("@HORAINICIO", horarioMarcado.HoraInicio);
            cmd.Parameters.AddWithValue("@DIASEMANA", horarioMarcado.DiaSemana);
            try
            {
                using (NpgsqlDataReader rd = cmd.ExecuteReader())
                {
                    while (rd.Read())
                    {
                        quantidade = Int32.Parse(rd[0].ToString());
                    }
                }
            }
            catch (NpgsqlException e)
            {
                Console.WriteLine("Erro " + e);
            }
            return quantidade;
        }


        internal int ContaHorariosEditMatricula(Matricula matricula)
        {
            int quantidade = 0;
            var instrucao = string.Format("select count (*) from horario_marcado where fkmatricula iLIKE @PKMATRICULA");
            var cmd = new NpgsqlCommand(instrucao, conn);
            cmd.Parameters.AddWithValue("@PKMATRICULA", "%" + matricula.PkMatricula.Trim() + "%");
            try
            {
                using (NpgsqlDataReader rd = cmd.ExecuteReader())
                {
                    while (rd.Read())
                    {
                        quantidade = Int32.Parse(rd[0].ToString());
                    }
                }
            }
            catch (NpgsqlException e)
            {
                Console.WriteLine("Erro " + e);
            }
            return quantidade;
        }


        internal List<HorariosList> VerificaHorarioDisponivelList(List<HorariosList> horarioDisponivelList)
        {
            List<HorariosList> horarioDisponivelListRetorno = new List<HorariosList>();
            for (int i = 0; i < horarioDisponivelList.Count; ++i)
            {
                var instrucao = string.Format("select count (*) from horario_disponivel where fkprofessor=@FKPROFESSOR and horainicio=@HORAINICIO and diasemana=@DIASEMANA");
                var cmd = new NpgsqlCommand(instrucao, conn);
                cmd.Parameters.AddWithValue("@FKPROFESSOR", horarioDisponivelList[i].HorarioDisponivel.FkProfessor.Trim());
                cmd.Parameters.AddWithValue("@HORAINICIO", horarioDisponivelList[i].HorarioDisponivel.HoraInicio);
                cmd.Parameters.AddWithValue("@DIASEMANA", horarioDisponivelList[i].HorarioDisponivel.DiaSemana);
                try
                {
                    using (NpgsqlDataReader rd = cmd.ExecuteReader())
                    {
                        while (rd.Read())
                        {
                            var horarioDisponivel4List = new HorariosList
                            {
                                HorarioDisponivel = horarioDisponivelList[i].HorarioDisponivel,
                                ILinha = horarioDisponivelList[i].ILinha,
                                IColuna = horarioDisponivelList[i].IColuna,
                                Quantidade = Int32.Parse(rd[0].ToString()),
                            };
                            horarioDisponivelListRetorno.Add(horarioDisponivel4List);
                        }
                    }
                }
                catch (NpgsqlException e)
                {
                    Console.WriteLine("Erro " + e);
                }
            }
            return horarioDisponivelListRetorno;
        }


        internal List<HorariosList> VerificaHorarioMarcadoList(List<HorariosList> horarioDisponivelList)
        {
            List<HorariosList> horarioDisponivelListRetorno = new List<HorariosList>();
            for (int i = 0; i < horarioDisponivelList.Count; ++i)
            {
                var instrucao = string.Format("select count (*) from horario_marcado where fkprofessor=@FKPROFESSOR and horainicio=@HORAINICIO and diasemana=@DIASEMANA");
                var cmd = new NpgsqlCommand(instrucao, conn);
                cmd.Parameters.AddWithValue("@FKPROFESSOR", horarioDisponivelList[i].HorarioDisponivel.FkProfessor.Trim());
                cmd.Parameters.AddWithValue("@HORAINICIO", horarioDisponivelList[i].HorarioDisponivel.HoraInicio);
                cmd.Parameters.AddWithValue("@DIASEMANA", horarioDisponivelList[i].HorarioDisponivel.DiaSemana);
                try
                {
                    using (NpgsqlDataReader rd = cmd.ExecuteReader())
                    {
                        while (rd.Read())
                        {
                            var horarioDisponivel4List = new HorariosList
                            {
                                HorarioDisponivel = horarioDisponivelList[i].HorarioDisponivel,
                                ILinha = horarioDisponivelList[i].ILinha,
                                IColuna = horarioDisponivelList[i].IColuna,
                                Quantidade = Int32.Parse(rd[0].ToString()),
                            };
                            horarioDisponivelListRetorno.Add(horarioDisponivel4List);
                        }
                    }
                }
                catch (NpgsqlException e)
                {
                    Console.WriteLine("Erro " + e);
                }
            }
            return horarioDisponivelListRetorno;
        }

        internal bool DelHorarioDisponivel(HorarioDisponivel horarioDisponivel)
        {
            bool ok = true;
            var instrucao = string.Format("delete from horario_disponivel where fkprofessor=@FKPROFESSOR and diasemana=@DIASEMANA and horainicio=@HORAINICIO");
            var cmd = new NpgsqlCommand(instrucao, conn);
            cmd.Parameters.AddWithValue("@FKPROFESSOR", horarioDisponivel.FkProfessor);
            cmd.Parameters.AddWithValue("@HORAINICIO", horarioDisponivel.HoraInicio);
            cmd.Parameters.AddWithValue("@DIASEMANA", horarioDisponivel.DiaSemana);
            try
            {
                if (cmd.ExecuteNonQuery() != 1)
                {
                    Console.WriteLine("Algo não deu muito certo ao deletar horário disponível");
                    ok = false;
                }
            }
            catch (NpgsqlException e)
            {
                Console.WriteLine("Erro " + e);
                ok = false;
            }
            return ok;
        }

        internal int VerificaHorarioDisponivel(HorarioMarcado horarioMarcado)
        {
            int quantidade = 0;
            var instrucao = string.Format("select count (*) from horario_disponivel where fkprofessor= @FKPROFESSOR and horainicio= @HORAINICIO and diasemana= @DIASEMANA");
            var cmd = new NpgsqlCommand(instrucao, conn);
            cmd.Parameters.AddWithValue("@FKPROFESSOR", horarioMarcado.FkProfessor.Trim());
            cmd.Parameters.AddWithValue("@HORAINICIO", horarioMarcado.HoraInicio);
            cmd.Parameters.AddWithValue("@DIASEMANA", horarioMarcado.DiaSemana);
            try
            {
                using (NpgsqlDataReader rd = cmd.ExecuteReader())
                {
                    while (rd.Read())
                    {
                        quantidade = Int32.Parse(rd[0].ToString());
                    }
                }
            }
            catch (NpgsqlException e)
            {
                Console.WriteLine("Erro " + e);
            }
            return quantidade;
        }

        internal int ContaHorariosAluno(Matricula matricula)
        {
            int quantidade = 0;
            var instrucao = string.Format("select count (*) from horario_marcado where fkmatricula=@FKMATRICULA");
            var cmd = new NpgsqlCommand(instrucao, conn);
            cmd.Parameters.AddWithValue("@FKMATRICULA", matricula.PkMatricula.Trim());
            try
            {
                using (NpgsqlDataReader rd = cmd.ExecuteReader())
                {
                    while (rd.Read())
                    {
                        quantidade = Int32.Parse(rd[0].ToString());
                    }
                }
            }
            catch (NpgsqlException e)
            {
                Console.WriteLine("Erro " + e);
            }
            return quantidade;
        }




        internal bool NewMatricula(Matricula matricula)
        {
            bool ok = true;
            string instrucao;
            if (matricula.Responsavel != null)
            {
                instrucao = string.Format("insert into matricula (pkmatricula,fkaluno,data,fkresponsavel,fkcurso,fkprofessor, valor,nivel, status) values (@PKMATRICULA,@FKALUNO,@DATA,@FKRESPONSAVEL,@FKCURSO,@FKPROFESSOR, @VALOR,@NIVEL, true)");
            }
            else
            {
                instrucao = string.Format("insert into matricula (pkmatricula,fkaluno,data,fkcurso,fkprofessor, valor,nivel,status) values (@PKMATRICULA, @FKALUNO, @DATA, @FKCURSO, @FKPROFESSOR, @VALOR, @NIVEL, true)");
            }
            var cmd = new NpgsqlCommand(instrucao, conn);
            cmd.Parameters.AddWithValue("@PKMATRICULA", matricula.PkMatricula.Trim());
            cmd.Parameters.AddWithValue("@FKALUNO", matricula.Aluno.PkPessoa.Trim());
            cmd.Parameters.AddWithValue("@DATA", matricula.Data);
            cmd.Parameters.AddWithValue("@FKCURSO", matricula.Curso.PkCurso.Trim());
            cmd.Parameters.AddWithValue("@FKPROFESSOR", matricula.Professor.PkPessoa.Trim());
            cmd.Parameters.AddWithValue("@VALOR", matricula.Valor);
            cmd.Parameters.AddWithValue("@NIVEL", matricula.Nivel.Trim());
            if (matricula.Responsavel != null)
            {
                cmd.Parameters.AddWithValue("@FKRESPONSAVEL", matricula.Responsavel.PkPessoa.Trim());
            }


            try
            {
                if (cmd.ExecuteNonQuery() != 1)
                {
                    Console.WriteLine("Algo não deu muito certo ao criar matrícula");
                    ok = false;
                }
            }
            catch (NpgsqlException e)
            {
                Console.WriteLine("Erro " + e);
                ok = false;
            }
            return ok;
        }


        internal MatriculaListItem[] FiltraMatricula(FiltroMatricula filtro)
        {
            var listMatricula = new List<MatriculaListItem>();
            string instrucao = string.Format("SELECT aluno.nome AS nomealuno, data, professor.nome as nomeprofessor, curso.instrumento as curso, curso.frequencia, pkmatricula, fkaluno, fkcurso, fkprofessor, matricula.fkresponsavel, matricula.valor, matricula.nivel, matricula.status, matricula.dtrescisao, matricula.motivorescisao FROM matricula INNER JOIN pessoa aluno ON aluno.pkpessoa = matricula.fkaluno INNER JOIN pessoa professor on professor.pkpessoa = matricula.fkprofessor INNER JOIN curso on curso.pkcurso = matricula.fkcurso where aluno.nome iLIKE @NOMEALUNO and professor.nome iLIKE @NOMEPROFESSOR and matricula.status=@STATUS and curso.instrumento iLIKE @INSTRUMENTO and data BETWEEN @DATAINICIO and @DATAFIM order by data desc, aluno.nome asc");
            var cmd = new NpgsqlCommand(instrucao, conn);
            cmd.Parameters.AddWithValue("@NOMEALUNO", "%" + filtro.Aluno.Trim() + "%");
            cmd.Parameters.AddWithValue("@NOMEPROFESSOR", "%" + filtro.Professor.Trim() + "%");
            cmd.Parameters.AddWithValue("@INSTRUMENTO", "%" + filtro.Curso.Trim() + "%");
            cmd.Parameters.AddWithValue("@DATAINICIO", filtro.DataInicio);
            cmd.Parameters.AddWithValue("@DATAFIM", filtro.DataFim);
            cmd.Parameters.AddWithValue("@STATUS", filtro.Status);
            try
            {
                using (NpgsqlDataReader rd = cmd.ExecuteReader())
                {
                    const int NOMEALUNO = 0;
                    const int Data = 1;
                    const int NOMEPROFESSOR = 2;
                    const int NOMECURSO = 3;
                    const int FREQUENCIACURSO = 4;
                    const int PKMATRICULA = 5;
                    const int FKALUNO = 6;
                    const int FKCURSO = 7;
                    const int FKPROFESSOR = 8;
                    const int FKRESPONSAVEL = 9;
                    const int VALORMATRICULA = 10;
                    const int NIVELMATRICULA = 11;
                    const int STATUS = 12;
                    const int DTRESCISAO = 13;
                    const int MOTIVORESCISAO = 14;

                    while (rd.Read())
                    {
                        var matriculaList = new MatriculaListItem
                        {
                            PkMatricula = rd[PKMATRICULA].ToString(),
                            NomeAluno = rd[NOMEALUNO].ToString(),
                            FkAluno = rd[FKALUNO].ToString(),
                            NomeProfessor = rd[NOMEPROFESSOR].ToString(),
                            FkProfessor = rd[FKPROFESSOR].ToString(),
                            NomeCurso = rd[NOMECURSO].ToString(),
                            FkCurso = rd[FKCURSO].ToString(),
                            Data = Convert.ToDateTime(rd[Data].ToString()),
                            FkResponsavel = rd[FKRESPONSAVEL].ToString(),
                            FrequenciaCurso = Int32.Parse(rd[FREQUENCIACURSO].ToString()),
                            Valor = Double.Parse(rd[VALORMATRICULA].ToString().Replace(".", ",").Trim()),
                            Nivel = rd[NIVELMATRICULA].ToString(),
                            Status = Convert.ToBoolean(rd[STATUS].ToString()),
                        };
                        if (!string.IsNullOrEmpty(rd[DTRESCISAO].ToString()))
                        {
                            matriculaList.DtRescisao = Convert.ToDateTime(rd[Data].ToString());
                            matriculaList.MotivoRescisao = rd[MOTIVORESCISAO].ToString();
                        }

                        listMatricula.Add(matriculaList);
                    }
                }

            }
            catch (NpgsqlException e)
            {
                Console.WriteLine("Erro " + e);
            }
            return listMatricula.ToArray();
        }

        internal Matricula FindMatricula(MatriculaListItem matriculaListItem)
        {
            Matricula matricula = new Matricula();
            string saveFkCurso = "";
            string saveFkAluno = "";
            string saveFkProfessor = "";
            string saveFkResponsavel = "";
            var instrucao = string.Format("select * from matricula where pkmatricula=@PKMATRICULA");
            var cmd = new NpgsqlCommand(instrucao, conn);
            cmd.Parameters.AddWithValue("@PKMATRICULA", matriculaListItem.PkMatricula.Trim());
            try
            {
                using (NpgsqlDataReader rd = cmd.ExecuteReader())
                {
                    while (rd.Read())
                    {
                        const int PKMATRICULA = 0;
                        const int FKALUNO = 1;
                        const int DATA = 2;
                        const int FKRESPONSAVEL = 3;
                        const int FKCURSO = 4;
                        const int FKPROFESSOR = 5;
                        const int VALOR = 6;
                        const int NIVEL = 7;
                        const int STATUS = 8;
                        const int DTRESCISAO = 9;

                        matricula.PkMatricula = rd[PKMATRICULA].ToString();
                        matricula.Data = DateTime.Parse(rd[DATA].ToString());
                        if (!String.IsNullOrEmpty(rd[DTRESCISAO].ToString()))
                        {
                            matricula.DtRescisao = DateTime.Parse(rd[DTRESCISAO].ToString());
                        }
                        matricula.Status = Convert.ToBoolean(rd[STATUS].ToString());
                        matricula.Nivel = rd[NIVEL].ToString();
                        matricula.Valor = Convert.ToDouble(rd[VALOR].ToString());
                        saveFkCurso = rd[FKCURSO].ToString();
                        saveFkAluno = rd[FKALUNO].ToString();
                        saveFkResponsavel = rd[FKRESPONSAVEL].ToString();
                        saveFkProfessor = rd[FKPROFESSOR].ToString();
                    }
                }
            }
            catch (NpgsqlException e)
            {
                Console.WriteLine("Erro " + e);
            }

            matricula.Aluno = FindPessoaPk(saveFkAluno);
            matricula.Professor = FindPessoaPk(saveFkProfessor);
            if (!string.IsNullOrEmpty(saveFkResponsavel))
            {
                matricula.Responsavel = FindPessoaPk(saveFkResponsavel);
            }
            matricula.Curso = FindCurso(saveFkCurso);
            return matricula;
        }
        internal int ContaMatricula(Matricula matricula)
        {
            int quantidade = 0;
            string instrucao = string.Format("select count (*) FROM matricula INNER JOIN pessoa aluno ON aluno.pkpessoa = matricula.fkaluno INNER JOIN pessoa professor on professor.pkpessoa = matricula.fkprofessor INNER JOIN curso on curso.pkcurso = matricula.fkcurso where aluno.nome iLIKE @NOMEALUNO and professor.nome iLIKE @NOMEPROFESSOR and curso.instrumento iLIKE @INSTRUMENTO and matricula.status=true");
            var cmd = new NpgsqlCommand(instrucao, conn);
            cmd.Parameters.AddWithValue("@NOMEALUNO", "%" + matricula.Aluno.Nome.Trim() + "%");
            cmd.Parameters.AddWithValue("@NOMEPROFESSOR", "%" + matricula.Professor.Nome.Trim() + "%");
            cmd.Parameters.AddWithValue("@INSTRUMENTO", "%" + matricula.Curso.Instrumento.Trim() + "%");

            try
            {
                using (NpgsqlDataReader rd = cmd.ExecuteReader())
                {
                    while (rd.Read())
                    {
                        quantidade = Int32.Parse(rd[0].ToString());
                    }
                }
            }
            catch (NpgsqlException e)
            {
                Console.WriteLine("Erro " + e);
            }
            Console.WriteLine(quantidade);
            return quantidade;
        }

        internal int ContaAtividade(Matricula matricula)
        {
            int quantidade = 0;
            string instrucao = string.Format("select count (*) FROM atividade where fkmatricula= @FKMATRICULA and data= @DATA");
            var cmd = new NpgsqlCommand(instrucao, conn);
            cmd.Parameters.AddWithValue("@FKMATRICULA", matricula.PkMatricula.Trim());
            cmd.Parameters.AddWithValue("@DATA", DateTime.Now.Date);
            try
            {
                using (NpgsqlDataReader rd = cmd.ExecuteReader())
                {
                    while (rd.Read())
                    {
                        quantidade = Int32.Parse(rd[0].ToString());
                    }
                }
            }
            catch (NpgsqlException e)
            {
                Console.WriteLine("Erro " + e);
            }
            return quantidade;
        }



        internal int ContaFalta(Falta falta)
        {
            int quantidade = 0;
            string instrucao = string.Format("select count (*) FROM falta where fkmatricula = @FKMATRICULA and data= @DATA");
            var cmd = new NpgsqlCommand(instrucao, conn);
            cmd.Parameters.AddWithValue("@FKMATRICULA", falta.MatriculaAluno.PkMatricula.Trim());
            cmd.Parameters.AddWithValue("@DATA", falta.Data.Date);

            try
            {
                using (NpgsqlDataReader rd = cmd.ExecuteReader())
                {
                    while (rd.Read())
                    {
                        quantidade = Int32.Parse(rd[0].ToString());
                    }
                }
            }
            catch (NpgsqlException e)
            {
                Console.WriteLine("Erro " + e);
            }

            return quantidade;
        }








        internal bool EditMatricula(Matricula matricula)
        {
            bool ok = true;
            String instrucao;
            if (matricula.Responsavel != null)
            {
                instrucao = string.Format("update matricula set fkaluno = @FKALUNO, fkresponsavel = @FKRESPONSAVEL, fkcurso = @FKCURSO, fkprofessor = @FKPROFESSOR, valor = @VALOR, nivel= @NIVEL, status= @STATUS where pkmatricula = @PKMATRICULA");
            }
            else
            {
                instrucao = string.Format("update matricula set fkaluno = @FKALUNO, fkresponsavel = null, fkcurso = @FKCURSO, fkprofessor = @FKPROFESSOR, valor = @VALOR, nivel= @NIVEL, status= @STATUS where pkmatricula = @PKMATRICULA");
            }
            var cmd = new NpgsqlCommand(instrucao, conn);
            cmd.Parameters.AddWithValue("@PKMATRICULA", matricula.PkMatricula.Trim());
            cmd.Parameters.AddWithValue("@FKALUNO", matricula.Aluno.PkPessoa.Trim());
            cmd.Parameters.AddWithValue("@DATA", matricula.Data);
            cmd.Parameters.AddWithValue("@FKCURSO", matricula.Curso.PkCurso.Trim());
            cmd.Parameters.AddWithValue("@FKPROFESSOR", matricula.Professor.PkPessoa.Trim());
            cmd.Parameters.AddWithValue("@VALOR", matricula.Valor);
            cmd.Parameters.AddWithValue("@NIVEL", matricula.Nivel.Trim());
            cmd.Parameters.AddWithValue("@STATUS", matricula.Status);

            if (matricula.Responsavel != null)
            {
                cmd.Parameters.AddWithValue("@FKRESPONSAVEL", matricula.Responsavel.PkPessoa.Trim());
            }

            try
            {
                if (cmd.ExecuteNonQuery() != 1)
                {
                    Console.WriteLine("Algo não deu muito certo ao editar matrícula");
                    ok = false;
                }
            }
            catch (NpgsqlException e)
            {
                Console.WriteLine("Erro " + e);
                ok = false;
            }
            return ok;
        }

        internal bool DelHorariosMarcados(Matricula matricula)
        {
            bool ok = true;
            String instrucao;
            instrucao = string.Format("delete from horario_marcado where fkmatricula= @FKMATRICULA");
            var cmd = new NpgsqlCommand(instrucao, conn);
            cmd.Parameters.AddWithValue("@FKMATRICULA", matricula.PkMatricula.Trim());
            try
            {
                cmd.ExecuteNonQuery();
            }
            catch (NpgsqlException e)
            {
                Console.WriteLine("Erro " + e);
                ok = false;
            }
            return ok;
        }
        
        internal bool DelReposicoes()
        {
            bool ok = true;
            String instrucao;
            instrucao = string.Format("delete from horario_marcado where reposicao=true and diasemana= @HOJE");
            var cmd = new NpgsqlCommand(instrucao, conn);
            cmd.Parameters.AddWithValue("@HOJE", ((int)DateTime.Now.DayOfWeek) + 1);
            try
            {
                cmd.ExecuteNonQuery();
            }
            catch (NpgsqlException e)
            {
                Console.WriteLine("Erro " + e);
                ok = false;
            }
            return ok;
        }

        internal bool NewAtividade(Atividade atividade)
        {
            bool ok = true;
            var instrucao = string.Format("insert into atividade (pkatividade, fkmatricula,  data, tipo, descricao, paracasa) values (@PKATIVIDADE, @FKMATRICULA,  @DATA, @TIPO, @DESCRICAO, @PARACASA)");
            var cmd = new NpgsqlCommand(instrucao, conn);
            cmd.Parameters.AddWithValue("@PKATIVIDADE", atividade.PkAtividade.Trim());
            cmd.Parameters.AddWithValue("@FKMATRICULA", atividade.MatriculaAluno.PkMatricula.Trim());
            cmd.Parameters.AddWithValue("@DATA", atividade.Data);
            cmd.Parameters.AddWithValue("@TIPO", atividade.Tipo.Trim());
            cmd.Parameters.AddWithValue("@DESCRICAO", atividade.Descricao.Trim());
            cmd.Parameters.AddWithValue("@PARACASA", atividade.ParaCasa.Trim());

            try
            {
                if (cmd.ExecuteNonQuery() != 1)
                {
                    Console.WriteLine("Algo não deu muito certo ao criar atividade.");
                    ok = false;
                }
            }
            catch (NpgsqlException e)
            {
                Console.WriteLine("Erro " + e);
                ok = false;
            }
            return ok;
        }

        internal bool NewEventoMusical(EventoMusical eventoMusical)
        {
            bool ok = true;
            var instrucao = string.Format("insert into eventomusical (pkeventomusical, data, local, descricao) values (@PKEVENTOMUSICAL, @DATA, @LOCAL, @DESCRICAO)");
            var cmd = new NpgsqlCommand(instrucao, conn);
            cmd.Parameters.AddWithValue("@PKEVENTOMUSICAL", eventoMusical.PkEventoMusical.Trim());
            cmd.Parameters.AddWithValue("@DATA", eventoMusical.Data);
            cmd.Parameters.AddWithValue("@LOCAL", eventoMusical.Local.Trim());
            cmd.Parameters.AddWithValue("@DESCRICAO", eventoMusical.Descricao.Trim());

            try
            {
                if (cmd.ExecuteNonQuery() != 1)
                {
                    Console.WriteLine("Algo não deu muito certo ao criar evento musical.");
                    ok = false;
                }
            }
            catch (NpgsqlException e)
            {
                Console.WriteLine("Erro " + e);
                ok = false;
            }
            return ok;
        }


        internal bool EditEventoMusical(EventoMusical eventoMusical)
        {
            bool ok = true;
            var instrucao = string.Format("update eventomusical set data=@DATA, local=@LOCAL, descricao=@DESCRICAO where pkeventomusical=@PKEVENTOMUSICAL ");
            var cmd = new NpgsqlCommand(instrucao, conn);
            cmd.Parameters.AddWithValue("@PKEVENTOMUSICAL", eventoMusical.PkEventoMusical.Trim());
            cmd.Parameters.AddWithValue("@DATA", eventoMusical.Data);
            cmd.Parameters.AddWithValue("@LOCAL", eventoMusical.Local.Trim());
            cmd.Parameters.AddWithValue("@DESCRICAO", eventoMusical.Descricao.Trim());
            try
            {
                if (cmd.ExecuteNonQuery() != 1)
                {
                    ok = false;
                }
            }
            catch (NpgsqlException e)
            {
                Console.WriteLine("Erro " + e);
                ok = false;
            }
            return ok;
        }

        internal bool NewPartEventoMusical(List<PartEvento> listPartEvento)
        {
            bool ok = true;
            for (int i = 0; i < listPartEvento.Count; ++i)
            {
                var instrucao = string.Format("insert into partevento (pkpartevento, fkpessoa, fkeventomusical, descricao) values (@PKPARTEVENTO, @FKPESSOA, @FKEVENTOMUSICAL, @DESCRICAO)");
                var cmd = new NpgsqlCommand(instrucao, conn);
                cmd.Parameters.AddWithValue("@PKPARTEVENTO", listPartEvento[i].PkPartEvento.Trim());
                cmd.Parameters.AddWithValue("@FKPESSOA", listPartEvento[i].Participante.PkPessoa);
                cmd.Parameters.AddWithValue("@FKEVENTOMUSICAL", listPartEvento[i].Evento.PkEventoMusical.Trim());
                cmd.Parameters.AddWithValue("@DESCRICAO", listPartEvento[i].Descricao.Trim());
                try
                {
                    if (cmd.ExecuteNonQuery() != 1)
                    {
                        Console.WriteLine("Algo não deu muito certo ao criar participação em evento musical.");
                        ok = false;
                    }
                }
                catch (NpgsqlException e)
                {
                    Console.WriteLine("Erro " + e);
                    ok = false;
                }
            }
            System.Diagnostics.Debug.WriteLine("7 - Inseriu " + listPartEvento.Count + " novos registros");
            return ok;
        }



        internal bool DelPartEventoMusical(EventoMusical eventoMusical)
        {
            bool ok = true;
            var instrucao = string.Format("delete from partevento where fkeventomusical=@FKEVENTOMUSICAL");
            var cmd = new NpgsqlCommand(instrucao, conn);
            cmd.Parameters.AddWithValue("@FKEVENTOMUSICAL", eventoMusical.PkEventoMusical.Trim());
            try
            {
                System.Diagnostics.Debug.WriteLine("4 - Deletou " + cmd.ExecuteNonQuery() + " registros");
                ok = true;
            }
            catch (NpgsqlException e)
            {
                Console.WriteLine("Erro " + e);
                ok = false;
            }
            return ok;
        }




        internal bool NewFalta(Falta falta)
        {
            bool ok = true;
            var instrucao = string.Format("insert into falta (pkfalta, fkmatricula,  data, justificativa,reposicao) values (@PKFALTA, @FKMATRICULA,  @DATA, @JUSTIFICATIVA,@REPOSICAO)");
            var cmd = new NpgsqlCommand(instrucao, conn);
            cmd.Parameters.AddWithValue("@PKFALTA", falta.PkFalta.Trim());
            cmd.Parameters.AddWithValue("@FKMATRICULA", falta.MatriculaAluno.PkMatricula.Trim());
            cmd.Parameters.AddWithValue("@DATA", falta.Data);
            cmd.Parameters.AddWithValue("@JUSTIFICATIVA", falta.Justificativa.Trim());
            cmd.Parameters.AddWithValue("@REPOSICAO", falta.Reposicao);
            try
            {
                if (cmd.ExecuteNonQuery() != 1)
                {
                    Console.WriteLine("Algo não deu muito certo ao criar falta.");
                    ok = false;
                }
            }
            catch (NpgsqlException e)
            {
                Console.WriteLine("Erro " + e);
                ok = false;
            }
            return ok;
        }


        internal Atividade[] FiltraAtividades(Matricula matricula)
        {
            var listAtividades = new List<Atividade>();
            string instrucao = string.Format("select * from atividade where fkmatricula=@PKMATRICULA order by data desc");
            var cmd = new NpgsqlCommand(instrucao, conn);
            cmd.Parameters.AddWithValue("@PKMATRICULA", matricula.PkMatricula.Trim());
            try
            {
                using (NpgsqlDataReader rd = cmd.ExecuteReader())
                {
                    while (rd.Read())
                    {

                        const int PKATIVIDADE = 0;
                        //const int FKMATRICULA = 1;
                        const int TIPO = 2;
                        const int DESCRICAO = 3;
                        const int DATA = 4;
                        const int PARACASA = 5;

                        Atividade atividade = new Atividade
                        {
                            Data = DateTime.Parse(rd[DATA].ToString()),
                            Descricao = rd[DESCRICAO].ToString(),
                            ParaCasa = rd[PARACASA].ToString(),
                            PkAtividade = rd[PKATIVIDADE].ToString(),
                            Tipo = rd[TIPO].ToString(),
                            MatriculaAluno = matricula,
                        };

                        listAtividades.Add(atividade);
                    }
                }

            }
            catch (NpgsqlException e)
            {
                Console.WriteLine("Erro " + e);
            }
            return listAtividades.ToArray();
        }


        internal Falta[] FiltraFaltas(Matricula matricula)
        {
            var listFaltas = new List<Falta>();
            string instrucao = string.Format("select * from falta where fkmatricula=@PKMATRICULA order by data desc");
            var cmd = new NpgsqlCommand(instrucao, conn);
            cmd.Parameters.AddWithValue("@PKMATRICULA", matricula.PkMatricula.Trim());
            try
            {
                using (NpgsqlDataReader rd = cmd.ExecuteReader())
                {
                    while (rd.Read())
                    {
                        const int PKFALTA = 0;
                        //    const int FKMATRICULA = 1;
                        const int DATA = 2;
                        const int JUSTIFICATIVA = 3;
                        const int REPOSICAO = 4;

                        Falta falta = new Falta
                        {
                            PkFalta = rd[PKFALTA].ToString(),
                            MatriculaAluno = matricula,
                            Data = DateTime.Parse(rd[DATA].ToString()),
                            Justificativa = rd[JUSTIFICATIVA].ToString(),
                            Reposicao = Convert.ToBoolean(rd[REPOSICAO].ToString()),
                        };

                        listFaltas.Add(falta);
                    }
                }

            }
            catch (NpgsqlException e)
            {
                Console.WriteLine("Erro " + e);
            }
            return listFaltas.ToArray();
        }

        internal bool EditAtividade(Atividade atividade)
        {
            bool ok = true;

            var instrucao = string.Format("update atividade set data=@DATA, tipo=@TIPO, descricao=@DESCRICAO, paracasa=@PARACASA where pkatividade=@PKATIVIDADE");
            var cmd = new NpgsqlCommand(instrucao, conn);
            cmd.Parameters.AddWithValue("@PKATIVIDADE", atividade.PkAtividade.Trim());
            cmd.Parameters.AddWithValue("@FKMATRICULA", atividade.MatriculaAluno.PkMatricula.Trim());
            cmd.Parameters.AddWithValue("@DATA", atividade.Data);
            cmd.Parameters.AddWithValue("@TIPO", atividade.Tipo.Trim());
            cmd.Parameters.AddWithValue("@DESCRICAO", atividade.Descricao.Trim());
            cmd.Parameters.AddWithValue("@PARACASA", atividade.ParaCasa.Trim());
            try
            {
                if (cmd.ExecuteNonQuery() != 1)
                {
                    Console.WriteLine("Algo não deu muito certo ao editar atividade.");
                    ok = false;
                }
            }
            catch (NpgsqlException e)
            {
                Console.WriteLine("Erro " + e);
                ok = false;
            }
            return ok;
        }


        internal bool EditFalta(Falta falta)
        {
            bool ok = true;
            var instrucao = string.Format("update falta set data= @DATA, justificativa= @JUSTIFICATIVA, reposicao= @REPOSICAO where pkfalta= @PKFALTA");
            var cmd = new NpgsqlCommand(instrucao, conn);
            cmd.Parameters.AddWithValue("@PKFALTA", falta.PkFalta.Trim());
            cmd.Parameters.AddWithValue("@FKMATRICULA", falta.MatriculaAluno.PkMatricula.Trim());
            cmd.Parameters.AddWithValue("@DATA", falta.Data);
            cmd.Parameters.AddWithValue("@JUSTIFICATIVA", falta.Justificativa.Trim());
            cmd.Parameters.AddWithValue("@REPOSICAO", falta.Reposicao);
            try
            {
                if (cmd.ExecuteNonQuery() != 1)
                {
                    Console.WriteLine("Algo não deu muito certo ao editar falta.");
                    ok = false;
                }
            }
            catch (NpgsqlException e)
            {
                Console.WriteLine("Erro " + e);
                ok = false;
            }
            return ok;
        }


        internal bool DelAtividade(Atividade atividade)
        {
            bool ok = true;
            var instrucao = string.Format("delete from atividade where pkatividade= @PKATIVIDADE");
            var cmd = new NpgsqlCommand(instrucao, conn);
            cmd.Parameters.AddWithValue("@PKATIVIDADE", atividade.PkAtividade.Trim());
            try
            {
                if (cmd.ExecuteNonQuery() != 1)
                {
                    Console.WriteLine("Algo não deu muito certo ao editar atividade.");
                    ok = false;
                }
            }
            catch (NpgsqlException e)
            {
                Console.WriteLine("Erro " + e);
                ok = false;
            }
            return ok;
        }


        internal bool DelFalta(Falta falta)
        {
            bool ok = true;
            var instrucao = string.Format("delete from falta where pkfalta=@PKFALTA");
            var cmd = new NpgsqlCommand(instrucao, conn);
            cmd.Parameters.AddWithValue("@PKFALTA", falta.PkFalta.Trim());
            try
            {
                if (cmd.ExecuteNonQuery() != 1)
                {
                    Console.WriteLine("Algo não deu muito certo ao excluir falta.");
                    ok = false;
                }
            }
            catch (NpgsqlException e)
            {
                Console.WriteLine("Erro " + e);
                ok = false;
            }
            return ok;
        }

        internal EventoMusical[] FiltraEventoMusical(FiltroEvento filtro)
        {
            var listEventoMusical = new List<EventoMusical>();
            string instrucao = string.Format("select distinct (eventomusical).* from pessoa inner join partevento on pkpessoa=partevento.fkpessoa inner join eventomusical on fkeventomusical=pkeventomusical where local iLIKE @LOCAL and eventomusical.descricao iLIKE @DESCRICAO and pessoa.pkpessoa iLIKE @PKPESSOA and eventomusical.data BETWEEN @DATAINICIO and @DATAFIM order by data desc");
            var cmd = new NpgsqlCommand(instrucao, conn);
            cmd.Parameters.AddWithValue("@LOCAL", "%" + filtro.Local.Trim() + "%");
            cmd.Parameters.AddWithValue("@DESCRICAO", "%" + filtro.Descricao.Trim() + "%");
            cmd.Parameters.AddWithValue("@PKPESSOA", "%" + filtro.Participante.PkPessoa.Trim() + "%");
            cmd.Parameters.AddWithValue("@DATAINICIO", filtro.DataInicio);
            cmd.Parameters.AddWithValue("@DATAFIM", filtro.DataFim);
            try
            {
                using (NpgsqlDataReader rd = cmd.ExecuteReader())
                {
                    const int PKEVENTOMUSICAL = 0;
                    const int DATA = 1;
                    const int LOCAL = 2;
                    const int DESCRICAO = 3;

                    while (rd.Read())
                    {
                        var eventoMusical = new EventoMusical
                        {
                            PkEventoMusical = rd[PKEVENTOMUSICAL].ToString(),
                            Descricao = rd[DESCRICAO].ToString(),
                            Local = rd[LOCAL].ToString(),
                            Data = Convert.ToDateTime(rd[DATA].ToString()),
                        };
                        listEventoMusical.Add(eventoMusical);
                    }
                }
            }
            catch (NpgsqlException e)
            {
                Console.WriteLine("Erro " + e);
            }
            return listEventoMusical.ToArray();
        }



        internal bool NewListPartitura(List<Partitura> listPartitura)
        {
            bool ok = true;
            for (int i = 0; i < listPartitura.Count; ++i)
            {
                var instrucao = string.Format("insert into partitura (pkpartitura, srvfilepath, nome, autor, instrumento, versao, pagina, playmusic,observacao,pasta) values (@PKPARTITURA, @SRVFILEPATH, @NOME, @AUTOR, @INSTRUMENTO, @VERSAO, @PAGINA, @PLAYMUSIC, @OBSERVACAO, @PASTA)");
                var cmd = new NpgsqlCommand(instrucao, conn);
                cmd.Parameters.AddWithValue("@PKPARTITURA", listPartitura[i].PkPartitura.Trim());
                cmd.Parameters.AddWithValue("@SRVFILEPATH", listPartitura[i].FkArquivo.Trim());
                cmd.Parameters.AddWithValue("@NOME", listPartitura[i].Nome.Trim());
                cmd.Parameters.AddWithValue("@AUTOR", listPartitura[i].Autor.Trim());
                cmd.Parameters.AddWithValue("@INSTRUMENTO", listPartitura[i].Instrumento.Trim());
                cmd.Parameters.AddWithValue("@VERSAO", listPartitura[i].Versao.Trim());
                cmd.Parameters.AddWithValue("@PAGINA", Int32.Parse(listPartitura[i].Pagina.Trim()));
                cmd.Parameters.AddWithValue("@PLAYMUSIC", listPartitura[i].Playmusic.Trim());
                cmd.Parameters.AddWithValue("@OBSERVACAO", listPartitura[i].Observacao.Trim());
                cmd.Parameters.AddWithValue("@PASTA", listPartitura[i].Pasta.Trim());
                try
                {
                    if (cmd.ExecuteNonQuery() != 1)
                    {
                        Console.WriteLine("Algo não deu muito certo ao cadastrar partituras.");
                        ok = false;
                    }
                }
                catch (NpgsqlException e)
                {
                    Console.WriteLine("Erro " + e);
                    ok = false;
                }
            }
            return ok;
        }

        internal bool DelPartituras(string SrvFilePath)
        {
            bool ok = true;
            var instrucao = string.Format("delete from partitura where srvfilepath =@SRVFILEPATH");
            var cmd = new NpgsqlCommand(instrucao, conn);
            cmd.Parameters.AddWithValue("@SRVFILEPATH", SrvFilePath.Trim());
            try
            {
                if (cmd.ExecuteNonQuery() == 0)
                {
                    Console.WriteLine("Algo não deu muito certo ao excluir as partituras.");
                    ok = false;
                }
            }
            catch (NpgsqlException e)
            {
                Console.WriteLine("Erro " + e);
                ok = false;
            }
            return ok;
        }


        internal Partitura[] ListFiltraPartitura(Partitura Partiturafiltro)
        {

            var listPartitura = new List<Partitura>();
            String instrucao = string.Format("select * from partitura where nome iLIKE @NOME and autor iLIKE @AUTOR and instrumento iLIKE @INSTRUMENTO and versao iLIKE @VERSAO and observacao iLIKE @OBSERVACAO and playmusic iLIKE @PLAYMUSIC and pasta iLIKE @PASTA and srvfilepath iLIKE @SRVFILEPATH order by nome asc");
            var cmd = new NpgsqlCommand(instrucao, conn);
            cmd.Parameters.AddWithValue("@PKPARTITURA", "%" + Partiturafiltro.PkPartitura.Trim() + "%");
            cmd.Parameters.AddWithValue("@SRVFILEPATH", "%" + Partiturafiltro.FkArquivo.Trim() + "%");
            cmd.Parameters.AddWithValue("@NOME", "%" + Partiturafiltro.Nome.Trim() + "%");
            cmd.Parameters.AddWithValue("@AUTOR", "%" + Partiturafiltro.Autor.Trim() + "%");
            cmd.Parameters.AddWithValue("@INSTRUMENTO", "%" + Partiturafiltro.Instrumento.Trim() + "%");
            cmd.Parameters.AddWithValue("@VERSAO", "%" + Partiturafiltro.Versao.Trim() + "%");
            cmd.Parameters.AddWithValue("@PLAYMUSIC", Partiturafiltro.Playmusic.Trim() + "%");
            cmd.Parameters.AddWithValue("@OBSERVACAO", "%" + Partiturafiltro.Observacao.Trim() + "%");
            cmd.Parameters.AddWithValue("@PASTA", "%" + Partiturafiltro.Pasta.Trim() + "%");
            try
            {
                using (NpgsqlDataReader rd = cmd.ExecuteReader())
                {
                    const int PKPARTITURA = 0;
                    const int SRVFILEPATH = 1;
                    const int NOME = 2;
                    const int AUTOR = 3;
                    const int INSTRUMENTO = 4;
                    const int VERSAO = 5;
                    const int PAGINA = 6;
                    const int OBSERVACAO = 7;
                    const int PASTA = 8;
                    const int PLAYMUSIC = 9;

                    while (rd.Read())
                    {
                        Partitura partitura = new Partitura
                        {
                            PkPartitura = rd[PKPARTITURA].ToString(),
                            FkArquivo = rd[SRVFILEPATH].ToString(),
                            Nome = rd[NOME].ToString(),
                            Autor = rd[AUTOR].ToString(),
                            Instrumento = rd[INSTRUMENTO].ToString(),
                            Versao = rd[VERSAO].ToString(),
                            Pagina = rd[PAGINA].ToString(),
                            Observacao = rd[OBSERVACAO].ToString(),
                            Pasta = rd[PASTA].ToString(),
                            Playmusic = rd[PLAYMUSIC].ToString(),
                        };
                        listPartitura.Add(partitura);
                    }
                }
            }
            catch (NpgsqlException e)
            {
                Console.WriteLine("Erro " + e);
            }
            return listPartitura.ToArray();
        }


        internal FaltaListItem[] FiltroAlertaFaltas()
        {
            var listFaltas = new List<FaltaListItem>();

            var listMatrDtAtv = new List<Atividade>();//List com todas as matrículas e respectiva última data de atividade registrada
            string instrucao = string.Format("select * from matricula where status=true");
            var cmd1 = new NpgsqlCommand(instrucao, conn);
            try
            {
                using (NpgsqlDataReader rd1 = cmd1.ExecuteReader())
                {
                    while (rd1.Read())
                    {
                        const int PKMATRICULA = 0;
                        const int DATAMATRICULA = 2;
                        Atividade atividade = new Atividade();
                        atividade.MatriculaAluno = new Matricula();
                        atividade.MatriculaAluno.PkMatricula = rd1[PKMATRICULA].ToString().Trim();
                        atividade.MatriculaAluno.Data = Convert.ToDateTime(rd1[DATAMATRICULA].ToString());
                        listMatrDtAtv.Add(atividade);
                    }
                }
            }
            catch (NpgsqlException e)
            {
                Console.WriteLine("Erro " + e);
            }

            for (int i = 0; i < listMatrDtAtv.Count; ++i)
            {
                instrucao = string.Format("select max (data) from atividade where fkmatricula= @MATRICULA");
                var cmd2 = new NpgsqlCommand(instrucao, conn);
                cmd2.Parameters.AddWithValue("@MATRICULA", listMatrDtAtv[i].MatriculaAluno.PkMatricula);
                try
                {
                    using (NpgsqlDataReader rd2 = cmd2.ExecuteReader())
                    {
                        while (rd2.Read())
                        {
                            const int DATAATIVIDADE = 0;
                            if (!string.IsNullOrEmpty(rd2[DATAATIVIDADE].ToString()))
                            {
                                listMatrDtAtv[i].Data = Convert.ToDateTime(rd2[DATAATIVIDADE].ToString());
                            }
                        }
                    }
                }
                catch (NpgsqlException e)
                {
                    Console.WriteLine("Erro " + e);
                }
            }


            for (int i = 0; i < listMatrDtAtv.Count; ++i)
            {
                instrucao = string.Format("select count (*) faltas, pkmatricula, matricula.fkaluno, aluno.nome as nomealuno, professor.nome as nomeprofessor, curso.instrumento, curso.frequencia from falta inner join matricula on fkmatricula = pkmatricula inner join pessoa aluno on fkaluno = aluno.pkpessoa inner join pessoa professor on fkprofessor = professor.pkpessoa inner join curso on pkcurso = fkcurso where justificativa = '' and falta.data > @DATA and falta.fkmatricula= @MATRICULA group by pkmatricula, aluno.nome, professor.nome, curso.instrumento, curso.frequencia, matricula.nivel order by nomealuno");
                var cmd3 = new NpgsqlCommand(instrucao, conn);
                cmd3.Parameters.AddWithValue("@MATRICULA", listMatrDtAtv[i].MatriculaAluno.PkMatricula.Trim());
                if (listMatrDtAtv[i].Data != new DateTime())
                {
                    cmd3.Parameters.AddWithValue("@DATA", listMatrDtAtv[i].Data.Date);
                }
                else
                {
                    cmd3.Parameters.AddWithValue("@DATA", listMatrDtAtv[i].MatriculaAluno.Data.Date);
                }
                try
                {
                    using (NpgsqlDataReader rd3 = cmd3.ExecuteReader())
                    {
                        const int FALTAS = 0;
                        const int FKMATRICULA = 1;
                        const int FKALUNO = 2;
                        const int NOMEALUNO = 3;
                        const int NOMEPROFESSOR = 4;
                        const int INSTRUMENTO = 5;
                        const int FREQUENCIACURSO = 6;

                        while (rd3.Read())
                        {
                            if ((Int32.Parse(rd3[FALTAS].ToString()) >= 2))
                            {
                                var faltaListItem = new FaltaListItem
                                {
                                    FkMatricula = rd3[FKMATRICULA].ToString(),
                                    FkAluno = rd3[FKALUNO].ToString(),
                                    NomeAluno = rd3[NOMEALUNO].ToString(),
                                    NomeProfessor = rd3[NOMEPROFESSOR].ToString(),
                                    Instrumento = rd3[INSTRUMENTO].ToString(),
                                    FrequenciaCurso = Int32.Parse(rd3[FREQUENCIACURSO].ToString()),
                                    Faltas = Int32.Parse(rd3[FALTAS].ToString()),

                                };
                                listFaltas.Add(faltaListItem);
                            }
                        }
                    }
                }
                catch (NpgsqlException e)
                {
                    Console.WriteLine("Erro " + e);
                }
            }
            return listFaltas.OrderByDescending(a => a.Faltas).ToArray();
        }


        internal Interatividade[] FiltraInteratividade()
        {
            var listInteratividade = new List<Interatividade>();//Últimos registros de falta ou atividade registrada

            var listAtividades = new List<Atividade>();//List com todas as matrículas e respectiva última data de atividade ou falta registrada
            string instrucao = string.Format("select * from matricula where status=true");
            var cmd1 = new NpgsqlCommand(instrucao, conn);
            try
            {
                using (NpgsqlDataReader rd1 = cmd1.ExecuteReader())
                {
                    while (rd1.Read())
                    {
                        const int PKMATRICULA = 0;
                        const int DATAMATRICULA = 2;
                        Atividade atividade = new Atividade();
                        atividade.MatriculaAluno = new Matricula();
                        atividade.MatriculaAluno.PkMatricula = rd1[PKMATRICULA].ToString().Trim();
                        atividade.MatriculaAluno.Data = Convert.ToDateTime(rd1[DATAMATRICULA].ToString());
                        listAtividades.Add(atividade);
                    }
                }
            }
            catch (NpgsqlException e)
            {
                Console.WriteLine("Erro " + e);
            }

            for (int i = 0; i < listAtividades.Count; ++i)
            {
                instrucao = string.Format("select max (data) from atividade where fkmatricula= @MATRICULA");
                var cmd2 = new NpgsqlCommand(instrucao, conn);
                cmd2.Parameters.AddWithValue("@MATRICULA", listAtividades[i].MatriculaAluno.PkMatricula);
                try
                {
                    using (NpgsqlDataReader rd2 = cmd2.ExecuteReader())
                    {
                        while (rd2.Read())
                        {
                            const int DATAATIVIDADE = 0;
                            if (!string.IsNullOrEmpty(rd2[DATAATIVIDADE].ToString()))
                            {
                                listAtividades[i].Data = Convert.ToDateTime(rd2[DATAATIVIDADE].ToString());
                            }
                            else
                            {
                                listAtividades[i].Data = listAtividades[i].MatriculaAluno.Data;
                            }
                        }
                    }
                }
                catch (NpgsqlException e)
                {
                    Console.WriteLine("Erro " + e);
                }
            }


            for (int i = 0; i < listAtividades.Count; ++i)
            {
                instrucao = string.Format("select max (data) from falta where fkmatricula= @MATRICULA");
                var cmd3 = new NpgsqlCommand(instrucao, conn);
                cmd3.Parameters.AddWithValue("@MATRICULA", listAtividades[i].MatriculaAluno.PkMatricula);
                try
                {
                    using (NpgsqlDataReader rd2 = cmd3.ExecuteReader())
                    {
                        while (rd2.Read())
                        {
                            const int DATAFALTA = 0;
                            if (!string.IsNullOrEmpty(rd2[DATAFALTA].ToString()))
                            {
                                if (listAtividades[i].Data < Convert.ToDateTime(rd2[DATAFALTA].ToString()))
                                {
                                    listAtividades[i].Data = Convert.ToDateTime(rd2[DATAFALTA].ToString());
                                }
                            }
                        }
                    }
                }
                catch (NpgsqlException e)
                {
                    Console.WriteLine("Erro " + e);
                }
            }


            for (int i = 0; i < listAtividades.Count; ++i)
            {
                instrucao = string.Format("select pkmatricula, matricula.fkaluno, aluno.nome as nomealuno, professor.nome as nomeprofessor, curso.instrumento, curso.frequencia from matricula inner join pessoa aluno on fkaluno = aluno.pkpessoa inner join pessoa professor on fkprofessor = professor.pkpessoa inner join curso on pkcurso = fkcurso where pkmatricula= @MATRICULA order by nomealuno");
                var cmd4 = new NpgsqlCommand(instrucao, conn);
                cmd4.Parameters.AddWithValue("@MATRICULA", listAtividades[i].MatriculaAluno.PkMatricula.Trim());

                try
                {
                    using (NpgsqlDataReader rd3 = cmd4.ExecuteReader())
                    {
                        const int PKMATRICULA = 0;
                        const int FKALUNO = 1;
                        const int NOMEALUNO = 2;
                        const int NOMEPROFESSOR = 3;
                        const int INSTRUMENTO = 4;
                        const int FREQUENCIACURSO = 5;

                        while (rd3.Read())
                        {

                            DateTime dt1 = listAtividades[i].Data.Date;
                            DateTime dt2 = DateTime.Today.Date;
                            TimeSpan resultado = dt2.Subtract(dt1);


                            if (resultado.Days >= 8)
                            {
                                var registros = new Interatividade
                                {
                                    FkMatricula = rd3[PKMATRICULA].ToString(),
                                    FkAluno = rd3[FKALUNO].ToString(),
                                    NomeAluno = rd3[NOMEALUNO].ToString(),
                                    NomeProfessor = rd3[NOMEPROFESSOR].ToString(),
                                    Instrumento = rd3[INSTRUMENTO].ToString(),
                                    FrequenciaCurso = Int32.Parse(rd3[FREQUENCIACURSO].ToString()),
                                    UltimoRegistro = listAtividades[i].Data,
                                    Contador = resultado.Days,
                                };
                                listInteratividade.Add(registros);
                            }
                        }
                    }
                }
                catch (NpgsqlException e)
                {
                    Console.WriteLine("Erro " + e);
                }
            }
            return listInteratividade.OrderByDescending(a => a.Contador).ToArray();
        }



        public string Backup()
        {


            DirectoryInfo directory = new DirectoryInfo(Properties.parametros.Default.DiretorioBackup);
            foreach (System.IO.FileInfo file in directory.GetFiles())
            {
                file.Delete();
            }


            try
            {
                Environment.SetEnvironmentVariable("PGPASSWORD", _password);
                DateTime Time = DateTime.Now;
                int year = Time.Year;
                int month = Time.Month;
                int day = Time.Day;
                int hour = Time.Hour;
                int minute = Time.Minute;
                int second = Time.Second;
                int millisecond = Time.Millisecond;
                string path;
                path = Properties.parametros.Default.DiretorioBackup+"BackupBanco -" + day + "-" + month + "-" + year + "-" + hour + "-" + minute + ".sql";
                StreamWriter file = new StreamWriter(path);
                ProcessStartInfo psi = new ProcessStartInfo();
                psi.FileName = Properties.parametros.Default.ArquivoDumpPostgre;
                psi.RedirectStandardInput = false;
                psi.RedirectStandardOutput = true;
                psi.Arguments = string.Format(@"-U{0} -p{1} -h{2} {3}", _user, _port, _server, _database);
                psi.UseShellExecute = false;


                Process process = Process.Start(psi);


                string output;
                output = process.StandardOutput.ReadToEnd();
                file.WriteLine(output);
                process.WaitForExit();
                file.Close();
                process.Close();
                return path;
            }
            catch (IOException ex)
            {
                Console.WriteLine("Error , unable to backup!");
                return null;
            }
        }



    }
}
